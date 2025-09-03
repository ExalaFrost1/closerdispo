"""
Campaign processing module - processes all clients within a single campaign
Updated to use Pub/Sub for database operations AND optimized with hash-first approach
"""
import asyncio
from typing import List, Any, Tuple, Dict
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
    HTTP_TIMEOUT, CONNECT_TIMEOUT,
    SKIP_TEMPORARY_STATUSES, TEMPORARY_STATUSES, LOG_SKIPPED_STATUSES
)


async def process_client_credentials(session, client, credentials, shutdown_event, pubsub_publisher=None):
    """Process leads for a single client's credentials - OPTIMIZED WITH EARLY HASH CHECK"""
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

            # STEP 1: Process CSV to get lead data (lead_id + call_time)
            lead_data = await process_csv_chunks(csv_path, shutdown_event)

            if not lead_data or shutdown_event.is_set():
                return 0

            # STEP 2: EARLY HASH GENERATION AND CHECKING (BEFORE WEB SCRAPING)
            leads_to_process = await filter_leads_by_hash_check(
                client, lead_data, campaign, client_id, shutdown_event
            )

            processed_count = 0
            if leads_to_process and not shutdown_event.is_set():
                # STEP 3: Only web scrape the leads that need processing
                processed_count = await process_filtered_leads_with_pubsub(
                    session, client, campaign, lead_details_url_template,
                    username, password, client_name, client_id, leads_to_process,
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


async def filter_leads_by_hash_check(client, lead_data: List[Tuple[str, str]],
                                   campaign: str, client_id: str, shutdown_event) -> Dict[str, List]:
    """
    Filter leads based on hash existence check - BEFORE web scraping
    Returns categorized leads: new, update_needed, skip
    """
    logger.info(f"Pre-filtering {len(lead_data)} leads using hash check for ClientID {client_id}")

    try:
        # STEP 1: Generate hashes for all leads (we have all needed data!)
        lead_hash_mapping = {}
        all_hash_values = []

        for lead_id, call_datetime in lead_data:
            if shutdown_event.is_set():
                break

            # Generate hash using available data (no web scraping needed!)
            hash_value = generate_hash(lead_id, call_datetime, client_id, campaign)
            lead_hash_mapping[hash_value] = {
                'lead_id': lead_id,
                'call_datetime': call_datetime,
                'hash_value': hash_value
            }
            all_hash_values.append(hash_value)

        if shutdown_event.is_set():
            return {'new_leads': [], 'update_leads': [], 'skip_leads': []}

        logger.info(f"Generated {len(all_hash_values)} hashes, checking existence in database...")

        # STEP 2: Batch check all hashes against database
        existing_hashes = await batch_check_existing_hashes_today(client, all_hash_values, shutdown_event)

        # STEP 3: Categorize leads based on existence
        new_leads = []          # Need full web scraping + insert
        update_leads = []       # Need web scraping + update (existing but incomplete)
        skip_leads = []         # Skip entirely (complete records exist)

        for hash_value, lead_info in lead_hash_mapping.items():
            if shutdown_event.is_set():
                break

            existing_record = existing_hashes.get(hash_value)

            if existing_record:
                # Record exists in database
                if existing_record.get('Length') is not None:
                    # Complete record exists - skip entirely
                    skip_leads.append(lead_info)
                else:
                    # Incomplete record exists - needs update
                    update_leads.append({
                        **lead_info,
                        'existing_record': existing_record
                    })
            else:
                # New record - needs full processing
                new_leads.append(lead_info)

        # Log the filtering results
        logger.info(f"Hash-based filtering results for ClientID {client_id}:")
        logger.info(f"  - New leads (need full processing): {len(new_leads)}")
        logger.info(f"  - Update leads (need web scraping): {len(update_leads)}")
        logger.info(f"  - Skip leads (already complete): {len(skip_leads)}")

        web_scraping_needed = len(new_leads) + len(update_leads)
        total_leads = len(lead_data)
        percentage_saved = ((total_leads - web_scraping_needed) / total_leads * 100) if total_leads > 0 else 0

        logger.info(f"  - Web scraping efficiency: {web_scraping_needed}/{total_leads} "
                   f"({percentage_saved:.1f}% reduction in web requests)")

        return {
            'new_leads': new_leads,
            'update_leads': update_leads,
            'skip_leads': skip_leads
        }

    except Exception as e:
        logger.error(f"Error in hash-based lead filtering: {e}")
        # Fallback: process all leads if filtering fails
        return {
            'new_leads': [{'lead_id': lid, 'call_datetime': ct, 'hash_value': generate_hash(lid, ct, client_id, campaign)}
                         for lid, ct in lead_data],
            'update_leads': [],
            'skip_leads': []
        }


async def process_filtered_leads_with_pubsub(session, client, campaign, lead_details_url_template,
                                           username, password, client_name, client_id,
                                           leads_to_process, shutdown_event, pubsub_publisher=None):
    """Process only the leads that need web scraping - MAXIMUM EFFICIENCY"""

    new_leads = leads_to_process['new_leads']
    update_leads = leads_to_process['update_leads']
    skip_count = len(leads_to_process['skip_leads'])

    total_to_scrape = len(new_leads) + len(update_leads)

    if total_to_scrape == 0:
        logger.info(f"No web scraping needed for ClientID {client_id} - all {skip_count} records already complete")
        return skip_count

    # Use provided publisher or create a new one
    if pubsub_publisher is None:
        async with get_pubsub_publisher() as publisher:
            return await process_filtered_leads_with_pubsub(
                session, client, campaign, lead_details_url_template,
                username, password, client_name, client_id, leads_to_process,
                shutdown_event, publisher
            )

    logger.info(f"Web scraping {total_to_scrape} leads for ClientID {client_id} "
               f"({len(new_leads)} new + {len(update_leads)} updates)")

    processed_count = skip_count  # Count skipped records as "processed"
    db_batch = []

    try:
        # Combine leads that need web scraping
        all_leads_to_scrape = []

        # Add new leads
        for lead_info in new_leads:
            all_leads_to_scrape.append({
                'lead_id': lead_info['lead_id'],
                'call_datetime': lead_info['call_datetime'],
                'hash_value': lead_info['hash_value'],
                'action': 'insert'
            })

        # Add update leads
        for lead_info in update_leads:
            all_leads_to_scrape.append({
                'lead_id': lead_info['lead_id'],
                'call_datetime': lead_info['call_datetime'],
                'hash_value': lead_info['hash_value'],
                'action': 'update',
                'existing_record': lead_info['existing_record']
            })

        # Process leads in concurrent batches
        for i in range(0, len(all_leads_to_scrape), CONCURRENT_REQUESTS):
            if shutdown_event.is_set():
                break

            batch_leads = all_leads_to_scrape[i:i + CONCURRENT_REQUESTS]

            logger.info(f"Web scraping batch {i // CONCURRENT_REQUESTS + 1} with {len(batch_leads)} leads")

            # Convert to format expected by process_lead_batch
            batch_data = [(lead['lead_id'], lead['call_datetime']) for lead in batch_leads]

            # Process this batch concurrently (web scraping)
            records = await process_lead_batch(
                session, lead_details_url_template, username, password,
                batch_data, campaign, client_id, shutdown_event
            )

            # Process each scraped record
            for j, record in enumerate(records):
                if shutdown_event.is_set():
                    break

                if j >= len(batch_leads):
                    continue

                lead_info = batch_leads[j]
                record['HashValue'] = lead_info['hash_value']

                # FILTER OUT TEMPORARY STATUSES - Skip INCALL and DISPO
                status = record.get('Status', '').upper().strip()
                if SKIP_TEMPORARY_STATUSES and status in [s.upper() for s in TEMPORARY_STATUSES]:
                    if LOG_SKIPPED_STATUSES:
                        logger.info(f"Skipping lead {record['Lead']} with temporary status: {status} - will retry in next cycle")
                    continue

                if lead_info['action'] == 'update':
                    # Send update message
                    await update_record_pubsub(pubsub_publisher, record['HashValue'], record, shutdown_event)
                    processed_count += 1
                    logger.debug(f"Sent update message for lead {record['Lead']}")

                elif lead_info['action'] == 'insert':
                    # Add to batch for new records
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

        # Log final summary
        logger.info(f"ClientID {client_id} processing summary:")
        logger.info(f"  - Records skipped (already complete): {skip_count}")
        logger.info(f"  - Records web scraped: {total_to_scrape}")
        logger.info(f"  - New records published: {len(new_leads)}")
        logger.info(f"  - Existing records updated: {len(update_leads)}")
        logger.info(f"  - Total processed: {processed_count}")

    except Exception as e:
        logger.error(f"Error in process_filtered_leads_with_pubsub for client {client_id}: {e}")

    return processed_count


async def process_campaign(campaign: str, credentials_list: List[Any], shutdown_event):
    """Process all clients in a single campaign - WITH OPTIMIZED HASH-FIRST APPROACH"""
    logger.info(f"Starting OPTIMIZED campaign processing: {campaign} with {len(credentials_list)} clients")

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
        logger.error(f"Error in optimized campaign {campaign}: {e}")

    logger.info(f"Optimized campaign {campaign} completed. Total processed: {total_processed}")
    return total_processed


# Legacy function for backward compatibility
async def process_leads_with_pubsub(session, client, campaign, lead_details_url_template,
                                   username, password, client_name, client_id, lead_data,
                                   shutdown_event, pubsub_publisher=None):
    """Legacy wrapper - converts old flow to new optimized flow"""
    logger.warning("Using legacy process_leads_with_pubsub - consider updating to optimized flow")

    # Convert lead_data to the format expected by the new functions
    leads_to_process = {
        'new_leads': [{'lead_id': lid, 'call_datetime': ct, 'hash_value': generate_hash(lid, ct, client_id, campaign)}
                     for lid, ct in lead_data],
        'update_leads': [],
        'skip_leads': []
    }

    return await process_filtered_leads_with_pubsub(
        session, client, campaign, lead_details_url_template,
        username, password, client_name, client_id, leads_to_process,
        shutdown_event, pubsub_publisher
    )


# Legacy function for even older compatibility
async def process_leads(session, client, campaign, lead_details_url_template,
                        username, password, client_name, client_id, lead_data, shutdown_event):
    """Legacy wrapper function - now uses optimized Pub/Sub flow with temporary status filtering"""
    return await process_leads_with_pubsub(
        session, client, campaign, lead_details_url_template,
        username, password, client_name, client_id, lead_data, shutdown_event
    )