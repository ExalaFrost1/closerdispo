"""
Campaign processing module - processes all clients within a single campaign
Updated to use Pub/Sub for database operations
"""
import asyncio
from typing import List, Any
import aiohttp
from urllib.parse import urlparse

from utils import (
    generate_hash, get_lead_details_url,
    update_csv_url_with_today_date, logger
)
from database import (
    get_bigquery_client, check_existing_hash,
    update_record_pubsub, save_to_pubsub_batch,
    batch_check_existing_hashes_today, get_pubsub_publisher
)
from data_fetcher import (
    download_csv_tempfile, process_csv_chunks,
    process_lead_batch
)
from config import (
    CONCURRENT_REQUESTS, DB_BATCH_SIZE,
    HTTP_TIMEOUT, CONNECT_TIMEOUT
)


async def process_client_credentials(session, client, credentials, shutdown_event, pubsub_publisher=None):
    """Process leads for a single client's credentials - WITH PUB/SUB SUPPORT"""
    campaign = credentials.Campaign
    csv_url = credentials.URL
    username = credentials.Username
    password = credentials.Password
    client_name = credentials.ClientName
    client_id = credentials.ClientID

    logger.info(f"Processing ClientID: {client_id} - {client_name}")

    try:
        if shutdown_event.is_set():
            return 0

        # Test connectivity first
        try:
            parsed_url = urlparse(csv_url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            auth = aiohttp.BasicAuth(username, password)

            test_timeout = aiohttp.ClientTimeout(total=CONNECT_TIMEOUT, connect=15)
            use_ssl = base_url.startswith('https://')
            async with session.head(base_url, auth=auth, ssl=use_ssl, timeout=test_timeout) as response:
                pass
            logger.debug(f"Connectivity test passed for {client_id}")
        except Exception as e:
            logger.warning(f"Connectivity test failed for {client_id}: {type(e).__name__}. Proceeding anyway...")

        lead_details_url_template = get_lead_details_url(csv_url)
        current_csv_url = update_csv_url_with_today_date(csv_url)

        async with download_csv_tempfile(session, current_csv_url, username, password, shutdown_event) as csv_path:
            if not csv_path or shutdown_event.is_set():
                return 0

            lead_data = await process_csv_chunks(csv_path, shutdown_event)

            processed_count = 0
            if lead_data and not shutdown_event.is_set():
                processed_count = await process_leads_with_pubsub(
                    session, client, campaign, lead_details_url_template,
                    username, password, client_name, client_id, lead_data,
                    shutdown_event, pubsub_publisher
                )

            return processed_count

    except (aiohttp.ClientConnectorError, aiohttp.ClientTimeout,
            aiohttp.ServerTimeoutError, asyncio.TimeoutError,
            ConnectionResetError, OSError) as e:
        logger.error(f"Network error processing client {client_id}: {type(e).__name__} - {e}")
        logger.info(f"Skipping client {client_id} due to network issues")
        return 0
    except Exception as e:
        logger.error(f"Error processing client {client_id}: {e}")
        return 0


async def process_leads_with_pubsub(session, client, campaign, lead_details_url_template,
                                   username, password, client_name, client_id, lead_data,
                                   shutdown_event, pubsub_publisher=None):
    """Process leads with their datetimes in concurrent batches - WITH PUB/SUB INTEGRATION"""
    if not lead_data or shutdown_event.is_set():
        logger.info(f"No valid leads to process for ClientID {client_id}")
        return 0

    processed_count = 0
    total_leads = len(lead_data)
    db_batch = []

    # Use provided publisher or create a new one
    if pubsub_publisher is None:
        async with get_pubsub_publisher() as publisher:
            return await process_leads_with_pubsub(
                session, client, campaign, lead_details_url_template,
                username, password, client_name, client_id, lead_data,
                shutdown_event, publisher
            )

    logger.info(f"Starting to process {total_leads} leads for ClientID {client_id}")

    try:
        # Process leads in concurrent batches
        all_records = []  # Collect all records first

        for i in range(0, len(lead_data), CONCURRENT_REQUESTS):
            if shutdown_event.is_set():
                break

            batch_data = lead_data[i:i + CONCURRENT_REQUESTS]

            # Process this batch concurrently
            logger.info(f"Processing batch {i // CONCURRENT_REQUESTS + 1} with {len(batch_data)} leads")
            records = await process_lead_batch(
                session, lead_details_url_template, username, password,
                batch_data, campaign, client_id, shutdown_event
            )

            # Generate hashes for all records in this batch
            for record in records:
                if shutdown_event.is_set():
                    break

                try:
                    # Generate hash for this record
                    hash_value = generate_hash(
                        record['Lead'], record['CallTime'], client_id, campaign
                    )
                    record['HashValue'] = hash_value
                    all_records.append(record)
                except Exception as e:
                    logger.error(f"Error generating hash for lead {record.get('Lead', 'unknown')}: {e}")
                    continue

        if shutdown_event.is_set() or not all_records:
            return processed_count

        logger.info(f"Generated {len(all_records)} records with hashes. Starting deduplication check...")

        # EFFICIENT BATCH DEDUPLICATION: Check all hashes at once
        all_hash_values = [record['HashValue'] for record in all_records]
        existing_hashes = await batch_check_existing_hashes_today(client, all_hash_values, shutdown_event)

        logger.info(f"Deduplication check complete: {len(existing_hashes)} records already exist in today's database")

        # Process each record based on existence check
        new_count = 0
        updated_count = 0
        skipped_count = 0

        for record in all_records:
            if shutdown_event.is_set():
                break

            hash_value = record['HashValue']
            existing_record = existing_hashes.get(hash_value)

            if existing_record:
                # Record with this hash already exists in TODAY'S database
                if existing_record['Length'] is not None:
                    # Skip if length already exists
                    skipped_count += 1
                    logger.debug(f"Skipping lead {record['Lead']} - already exists in today's database with length")
                    continue
                else:
                    # Update if we now have length
                    if record['Length'] is not None:
                        await update_record_pubsub(pubsub_publisher, hash_value, record, shutdown_event)
                        updated_count += 1
                        processed_count += 1
                        logger.debug(f"Sent update message for existing record: lead {record['Lead']}")
            else:
                # New record - add to batch for publishing
                db_batch.append({
                    'CallTime': record['CallTime'],
                    'Campaign': campaign,
                    'ClientName': client_name,
                    'ClientID': client_id,
                    'LeadID': record['Lead'],
                    'Phone': record['Phone'],
                    'Status': record['Status'],
                    'Ingroup': record['Ingroup'],
                    'Length': record['Length'],
                    'HashValue': record['HashValue']
                })
                new_count += 1

                # Publish to Pub/Sub when batch size reached
                if len(db_batch) >= DB_BATCH_SIZE:
                    if await save_to_pubsub_batch(pubsub_publisher, db_batch, shutdown_event):
                        processed_count += len(db_batch)
                        logger.info(f"Published {len(db_batch)} new records to Pub/Sub")
                    db_batch = []

        # Publish any remaining records
        if db_batch and not shutdown_event.is_set():
            if await save_to_pubsub_batch(pubsub_publisher, db_batch, shutdown_event):
                processed_count += len(db_batch)
                logger.info(f"Published final batch of {len(db_batch)} new records to Pub/Sub")

        # Log summary
        logger.info(f"ClientID {client_id} processing summary:")
        logger.info(f"  - Total leads processed: {len(all_records)}")
        logger.info(f"  - New records published: {new_count}")
        logger.info(f"  - Existing records updated: {updated_count}")
        logger.info(f"  - Records skipped (already complete): {skipped_count}")
        logger.info(f"  - Total Pub/Sub operations: {processed_count}")

    except Exception as e:
        logger.error(f"Error in process_leads_with_pubsub for client {client_id}: {e}")

    logger.info(f"Completed processing for ClientID {client_id}. Total processed: {processed_count}")
    return processed_count


async def process_campaign(campaign: str, credentials_list: List[Any], shutdown_event):
    """Process all clients in a single campaign - WITH SHARED PUB/SUB PUBLISHER"""
    logger.info(f"Starting campaign processing: {campaign} with {len(credentials_list)} clients")

    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS, limit_per_host=5)

    total_processed = 0

    try:
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with get_bigquery_client() as bq_client:
                # Create a single Pub/Sub publisher for the entire campaign
                async with get_pubsub_publisher() as pubsub_publisher:

                    # Process clients sequentially within the campaign
                    # (but campaigns run in parallel via multiprocessing)
                    for credentials in credentials_list:
                        if shutdown_event.is_set():
                            break

                        try:
                            processed_count = await process_client_credentials(
                                session, bq_client, credentials, shutdown_event, pubsub_publisher
                            )
                            total_processed += processed_count

                            # Small delay between clients to prevent overwhelming
                            await asyncio.sleep(1)

                        except Exception as e:
                            logger.error(f"Error processing client {credentials.ClientID} in campaign {campaign}: {e}")
                            continue

    except Exception as e:
        logger.error(f"Error in campaign {campaign}: {e}")

    logger.info(f"Campaign {campaign} completed. Total processed: {total_processed}")
    return total_processed


# Legacy function for backward compatibility
async def process_leads(session, client, campaign, lead_details_url_template,
                        username, password, client_name, client_id, lead_data, shutdown_event):
    """Legacy wrapper function - now uses Pub/Sub"""
    return await process_leads_with_pubsub(
        session, client, campaign, lead_details_url_template,
        username, password, client_name, client_id, lead_data, shutdown_event
    )