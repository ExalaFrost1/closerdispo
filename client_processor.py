"""
Optimized client processor for maximum performance
"""
import asyncio
import aiohttp
from typing import List, Dict, Any, Tuple
import time
from datetime import datetime

from utils import (
    generate_hash, get_lead_details_url,
    update_csv_url_with_today_date, logger
)
from database import (
    get_bigquery_client, save_to_bigquery_batch,
    check_existing_hash, update_record
)
from data_fetcher import (
    download_csv_tempfile, process_csv_chunks
)
from config import (
    LeadID_BATCH_SIZE, DB_BATCH_SIZE, CONCURRENT_REQUESTS,
    HTTP_TIMEOUT, CONNECT_TIMEOUT, MAX_CONNECTIONS_PER_HOST,
    ENABLE_PERFORMANCE_LOGGING, LOG_BATCH_PROGRESS,
    MEMORY_EFFICIENT_MODE
)


class PerformanceMonitor:
    """Monitor and log performance metrics"""

    def __init__(self, client_id: str):
        self.client_id = client_id
        self.start_time = time.time()
        self.leads_processed = 0
        self.batches_processed = 0
        self.db_writes = 0
        self.last_log_time = self.start_time

    def log_progress(self, leads_processed: int, total_leads: int, force: bool = False):
        """Log progress at intervals"""
        if not ENABLE_PERFORMANCE_LOGGING:
            return

        current_time = time.time()
        self.leads_processed = leads_processed

        # Log every 10 batches or when forced
        if force or (LOG_BATCH_PROGRESS and self.batches_processed % 10 == 0):
            elapsed = current_time - self.start_time
            rate = leads_processed / elapsed if elapsed > 0 else 0
            remaining = total_leads - leads_processed
            eta = remaining / rate if rate > 0 else 0

            logger.info(f"Client {self.client_id}: {leads_processed}/{total_leads} leads "
                        f"({leads_processed / total_leads * 100:.1f}%) "
                        f"Rate: {rate:.1f}/sec, ETA: {eta:.0f}s")

    def batch_completed(self):
        """Mark a batch as completed"""
        self.batches_processed += 1

    def db_write_completed(self):
        """Mark a database write as completed"""
        self.db_writes += 1

    def get_summary(self) -> Dict[str, Any]:
        """Get performance summary"""
        elapsed = time.time() - self.start_time
        return {
            'client_id': self.client_id,
            'elapsed_seconds': elapsed,
            'leads_processed': self.leads_processed,
            'batches_processed': self.batches_processed,
            'db_writes': self.db_writes,
            'leads_per_second': self.leads_processed / elapsed if elapsed > 0 else 0
        }


async def process_leads_optimized(session, client, campaign, lead_details_url_template,
                                  username, password, client_name, client_id, lead_data, shutdown_event):
    """Optimized lead processing with better batching and concurrency"""
    if not lead_data or shutdown_event.is_set():
        logger.info(f"No valid leads to process for ClientID {client_id}")
        return 0

    total_leads = len(lead_data)
    monitor = PerformanceMonitor(client_id)

    logger.info(f"Starting optimized processing for ClientID {client_id}: {total_leads} leads")

    processed_count = 0
    db_batch = []
    leads_processed = 0

    try:
        # Process leads in larger, more efficient batches
        for i in range(0, len(lead_data), LeadID_BATCH_SIZE):
            if shutdown_event.is_set():
                break

            batch_data = lead_data[i:i + LeadID_BATCH_SIZE]
            batch_start_time = time.time()

            # Process this batch with higher concurrency
            records = await process_lead_batch_optimized(
                session, lead_details_url_template, username, password,
                batch_data, campaign, client_id, shutdown_event
            )

            # Batch process database operations for efficiency
            new_records, updated_count = await process_records_batch(
                client, records, campaign, client_name, client_id, shutdown_event
            )

            # Add new records to batch
            db_batch.extend(new_records)
            processed_count += updated_count
            leads_processed += len(batch_data)

            # Write to database in larger batches for efficiency
            if len(db_batch) >= DB_BATCH_SIZE:
                if await save_to_bigquery_batch(client, db_batch, shutdown_event):
                    processed_count += len(db_batch)
                    monitor.db_write_completed()
                    if ENABLE_PERFORMANCE_LOGGING:
                        logger.info(f"Client {client_id}: Saved {len(db_batch)} records to DB")
                db_batch = []

            monitor.batch_completed()
            monitor.log_progress(leads_processed, total_leads)

            # Memory cleanup for large datasets
            if MEMORY_EFFICIENT_MODE and monitor.batches_processed % 20 == 0:
                import gc
                gc.collect()

        # Save any remaining records
        if db_batch and not shutdown_event.is_set():
            if await save_to_bigquery_batch(client, db_batch, shutdown_event):
                processed_count += len(db_batch)
                monitor.db_write_completed()
                logger.info(f"Client {client_id}: Saved final batch of {len(db_batch)} records")

        # Final performance summary
        if ENABLE_PERFORMANCE_LOGGING:
            summary = monitor.get_summary()
            logger.info(f"Client {client_id} completed: {summary['leads_processed']} leads in "
                        f"{summary['elapsed_seconds']:.1f}s ({summary['leads_per_second']:.1f}/sec)")

    except Exception as e:
        logger.error(f"Error in optimized processing for client {client_id}: {e}")

    return processed_count


async def process_lead_batch_optimized(session, lead_details_url_template, username, password,
                                       lead_data_batch, campaign, client_id, shutdown_event):
    """Process a batch of leads with optimized concurrency"""
    if shutdown_event.is_set():
        return []

    # Create semaphore to limit concurrent requests but allow more than before
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async def process_single_lead(lead_id, call_datetime):
        """Process a single lead with semaphore control"""
        if not lead_id or lead_id == '0' or shutdown_event.is_set():
            return None

        async with semaphore:
            try:
                from data_fetcher import get_closer_record_info
                lead_url = lead_details_url_template.format(lead_id=lead_id)
                result = await get_closer_record_info(
                    session, lead_url, username, password,
                    lead_id, call_datetime, shutdown_event
                )
                if result:
                    result['CallTime'] = call_datetime
                    result['Campaign'] = campaign
                    result['ClientID'] = client_id
                    result['HashValue'] = generate_hash(lead_id, call_datetime, client_id, campaign)
                return result
            except Exception as e:
                logger.debug(f"Error processing lead {lead_id}: {e}")
                return None

    # Create tasks for all leads in the batch
    tasks = [
        process_single_lead(lead_id, call_datetime)
        for lead_id, call_datetime in lead_data_batch
    ]

    if not tasks:
        return []

    # Process all tasks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out exceptions and None results
    valid_records = []
    for result in results:
        if result and not isinstance(result, Exception):
            valid_records.append(result)

    return valid_records


async def process_records_batch(client, records, campaign, client_name, client_id, shutdown_event):
    """Batch process records for database operations"""
    if not records or shutdown_event.is_set():
        return [], 0

    new_records = []
    updated_count = 0

    # Batch check existing hashes for efficiency
    hash_values = [record['HashValue'] for record in records]
    existing_hashes = await batch_check_existing_hashes(client, hash_values, shutdown_event)

    for record in records:
        if shutdown_event.is_set():
            break

        hash_value = record['HashValue']
        existing_record = existing_hashes.get(hash_value)

        if existing_record:
            # Record exists - update if needed
            if existing_record['Length'] is None and record['Length'] is not None:
                await update_record(client, hash_value, record, shutdown_event)
                updated_count += 1
        else:
            # New record
            new_records.append({
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

    return new_records, updated_count


async def batch_check_existing_hashes(client, hash_values, shutdown_event):
    """Check multiple hashes in a single query for efficiency"""
    if not hash_values or shutdown_event.is_set():
        return {}

    try:
        from utils import get_today_date_ny
        from datetime import timedelta

        today = get_today_date_ny()
        tomorrow = today + timedelta(days=1)

        # Create batch query for all hashes
        hash_list = "', '".join(hash_values)
        query = f"""
        SELECT LeadID, CallTime, ClientID, Campaign, Length, HashValue
        FROM `inflection-403908.confinality_vicidial.XferCloserDisposition`
        WHERE HashValue IN ('{hash_list}')
          AND CallTime >= '{today.strftime("%Y-%m-%d %H:%M:%S")}'
          AND CallTime < '{tomorrow.strftime("%Y-%m-%d %H:%M:%S")}'
        """

        query_job = client.query(query)
        results = query_job.result()

        # Convert to dictionary for quick lookup
        existing_records = {}
        for row in results:
            existing_records[row.HashValue] = {
                'LeadID': row.LeadID,
                'CallTime': row.CallTime,
                'ClientID': row.ClientID,
                'Campaign': row.Campaign,
                'Length': row.Length,
                'HashValue': row.HashValue
            }

        return existing_records

    except Exception as e:
        logger.error(f"Error in batch hash check: {e}")
        return {}


async def process_client_optimized(credentials, shutdown_event):
    """Optimized client processing function"""
    campaign = credentials.Campaign
    csv_url = credentials.URL
    username = credentials.Username
    password = credentials.Password
    client_name = credentials.ClientName
    client_id = credentials.ClientID

    logger.info(f"Starting optimized processing for ClientID: {client_id} - {client_name}")

    try:
        if shutdown_event.is_set():
            return 0

        # Create optimized HTTP session
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT, connect=CONNECT_TIMEOUT)
        connector = aiohttp.TCPConnector(
            limit=CONCURRENT_REQUESTS * 2,
            limit_per_host=MAX_CONNECTIONS_PER_HOST
        )

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with get_bigquery_client() as bq_client:

                lead_details_url_template = get_lead_details_url(csv_url)
                current_csv_url = update_csv_url_with_today_date(csv_url)

                async with download_csv_tempfile(session, current_csv_url, username, password,
                                                 shutdown_event) as csv_path:
                    if not csv_path or shutdown_event.is_set():
                        return 0

                    lead_data = await process_csv_chunks(csv_path, shutdown_event)

                    if lead_data and not shutdown_event.is_set():
                        processed_count = await process_leads_optimized(
                            session, bq_client, campaign, lead_details_url_template,
                            username, password, client_name, client_id, lead_data, shutdown_event
                        )
                        return processed_count

                    return 0

    except Exception as e:
        logger.error(f"Error in optimized client processing {client_id}: {e}")
        return 0