"""
Data fetching operations for web scraping and CSV processing
"""
import os
import csv
import re
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from typing import List, Tuple, Dict, Any, Optional
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from utils import (
    with_adaptive_retry, clean_lead_id, create_temp_file,
    temp_file_manager, logger
)
from config import (
    NY_TZ, TIME_TOLERANCE, CSV_CHUNKSIZE,
    HTTP_TIMEOUT, CONNECT_TIMEOUT, REQUEST_DELAY, CONCURRENT_REQUESTS
)


@with_adaptive_retry(max_retries=3, base_delay=5)  # Changed from @with_retry()
async def get_closer_record_info(session, lead_url, username, password, lead_id, target_datetime, shutdown_event=None):
    """Fetch closer record info that matches the target datetime with tolerance - WITH RATE LIMITING"""
    logger.debug(f"Fetching data for lead ID: {lead_id}")

    # ADD RATE LIMITING - Small delay before each request
    if REQUEST_DELAY > 0:
        await asyncio.sleep(REQUEST_DELAY)

    auth = aiohttp.BasicAuth(username, password)
    use_ssl = lead_url.startswith('https://')

    timeout = aiohttp.ClientTimeout(total=CONNECT_TIMEOUT)

    try:
        async with session.get(lead_url, auth=auth, ssl=use_ssl, timeout=timeout) as response:
            if response.status == 429:  # Rate limited
                logger.warning(f"Rate limited for lead {lead_id}, waiting longer...")
                await asyncio.sleep(5)  # Wait 5 seconds before retry
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=429,
                    message="Rate limited"
                )

            response.raise_for_status()
            html = await response.text()

    except (aiohttp.ClientConnectorError, aiohttp.ClientTimeout) as e:
        logger.warning(f"Network error for lead {lead_id}: {type(e).__name__}")
        raise
    except Exception as e:
        logger.warning(f"Error fetching lead {lead_id}: {type(e).__name__} - {e}")
        raise

    # Rest of the function remains the same...
    soup = BeautifulSoup(html, 'html.parser')

    # Extract phone number from "Lead information" line
    phone_number = None
    lead_info = soup.find(string=re.compile(r'Lead information:'))
    if lead_info:
        phone_match = re.search(r'Lead information:\s*-\s*(\d+)', lead_info)
        if phone_match:
            phone_number = phone_match.group(1)
            try:
                phone_number = int(phone_number)
            except ValueError:
                pass

    # Find the "CLOSER RECORDS FOR THIS LEAD" table
    closer_records_header = soup.find('b', string="CLOSER RECORDS FOR THIS LEAD:")
    if not closer_records_header:
        logger.debug(f"No closer records table found for lead {lead_id}")
        return None

    closer_records_table = closer_records_header.find_next('table')
    if not closer_records_table:
        logger.debug(f"No closer records table found for lead {lead_id}")
        return None

    # Extract all rows from the table
    rows = closer_records_table.find_all('tr', bgcolor=True)

    # Parse the rows to extract relevant data
    matched_record = None
    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 8:
            continue

        try:
            date_time_str = cols[1].text.strip()
            record_datetime = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")
            record_datetime = NY_TZ.localize(record_datetime)

            # Check if this record matches the target datetime within tolerance
            time_diff = abs((record_datetime - target_datetime).total_seconds())
            if time_diff <= TIME_TOLERANCE:
                try:
                    length = int(cols[2].text.strip())
                except (ValueError, IndexError):
                    logger.debug(f"No valid LENGTH found for lead {lead_id}")
                    length = None

                status = cols[3].text.strip()
                ingroup = cols[5].text.strip() if len(cols) > 5 else ""
                lead = int(cols[7].text.strip()) if len(cols) > 7 else None

                matched_record = {
                    'Length': length,
                    'Status': status,
                    'Ingroup': ingroup,
                    'Lead': lead,
                    'Phone': phone_number
                }
                break
        except Exception as e:
            logger.debug(f"Error parsing row for lead {lead_id}: {e}")
            continue

    return matched_record


@asynccontextmanager
async def download_csv_tempfile(session, url, username, password, shutdown_event):
    """Context manager for downloading CSV to temporary file"""
    temp_path = None
    temp_fd = None
    file_id = None

    try:
        logger.info(f"Starting download from URL")
        auth = aiohttp.BasicAuth(username, password)

        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
        use_ssl = url.startswith('https://')

        async with session.get(url, auth=auth, ssl=use_ssl, timeout=timeout) as response:
            response.raise_for_status()

            # Create secure temporary file
            temp_fd, temp_path, file_id = create_temp_file()
            logger.info(f"[File {file_id}] Creating temporary file")

            try:
                with os.fdopen(temp_fd, 'wb') as temp_file:
                    total_bytes = 0
                    async for chunk in response.content.iter_chunked(1024 * 1024):
                        if shutdown_event.is_set():
                            raise KeyboardInterrupt("Shutdown requested")
                        temp_file.write(chunk)
                        total_bytes += len(chunk)

                logger.info(f"[File {file_id}] Successfully saved {total_bytes / 1024 / 1024:.2f} MB")
                yield temp_path

            except Exception:
                # If we opened the file descriptor but failed, close it
                try:
                    if temp_fd:
                        os.close(temp_fd)
                except:
                    pass
                raise

    except Exception as e:
        logger.error(f"[File {file_id or 'unknown'}] Error downloading CSV: {e}")
        raise
    finally:
        if temp_path:
            try:
                if os.path.exists(temp_path):
                    file_size = os.path.getsize(temp_path)
                    logger.info(
                        f"[File {file_id or 'unknown'}] Removing temporary file (size: {file_size / 1024:.2f} KB)")
                    os.remove(temp_path)
                    logger.info(f"[File {file_id or 'unknown'}] Temporary file successfully removed")
                temp_file_manager.remove_temp_file(temp_path)
            except Exception as e:
                logger.error(f"[File {file_id or 'unknown'}] Error removing temporary file: {e}")


async def process_csv_chunks(csv_path, shutdown_event) -> List[Tuple[str, datetime]]:
    """Process CSV in chunks to avoid memory issues, returning lead IDs with their datetimes"""
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            skip_rows = 0
            headers_found = False
            date_col_idx = None
            lead_col_idx = None

            # Find header row and column indices
            for i, row in enumerate(reader):
                if shutdown_event.is_set():
                    return []
                if 'LEAD' in [col.upper() for col in row]:
                    headers = row
                    skip_rows = i
                    headers_found = True
                    date_col_idx = next((idx for idx, col in enumerate(row) if col.upper() == 'DATE/TIME'), None)
                    lead_col_idx = next((idx for idx, col in enumerate(row) if col.upper() == 'LEAD'), None)
                    break

        if not headers_found or date_col_idx is None or lead_col_idx is None:
            logger.error("Required columns (LEAD and DATE/TIME) not found in CSV")
            return []

        chunk_reader = pd.read_csv(csv_path, skiprows=skip_rows, chunksize=CSV_CHUNKSIZE)
        lead_data = []

        for chunk in chunk_reader:
            if shutdown_event.is_set():
                break

            if 'LEAD' in chunk.columns and 'DATE/TIME' in chunk.columns:
                chunk['LEAD'] = chunk['LEAD'].apply(clean_lead_id)
                valid_leads = chunk[chunk['LEAD'].notna()]

                for _, row in valid_leads.iterrows():
                    if shutdown_event.is_set():
                        break
                    try:
                        call_datetime = datetime.strptime(row['DATE/TIME'], "%Y-%m-%d %H:%M:%S")
                        call_datetime = NY_TZ.localize(call_datetime)
                        lead_data.append((row['LEAD'], call_datetime))
                    except ValueError as e:
                        logger.warning(f"Invalid date format for lead {row['LEAD']}: {row['DATE/TIME']}")
                        continue

        logger.info(f"Found {len(lead_data)} valid leads with datetimes in CSV")
        return lead_data

    except Exception as e:
        logger.error(f"Error processing CSV: {e}")
        return []


async def process_lead_batch(session, lead_details_url_template, username, password,
                             lead_data_batch, campaign, client_id, shutdown_event):
    """Process a batch of lead IDs with their datetimes concurrently - WITH BETTER ERROR HANDLING"""
    if shutdown_event.is_set():
        return []

    # Use a semaphore to limit concurrent requests more strictly
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)  # This will now be 5 instead of 50

    async def process_single_lead_safe(lead_id, call_datetime):
        """Process a single lead with semaphore control and better error handling"""
        if not lead_id or lead_id == '0' or shutdown_event.is_set():
            return None

        async with semaphore:
            try:
                lead_url = lead_details_url_template.format(lead_id=lead_id)
                result = await get_closer_record_info(
                    session, lead_url, username, password,
                    lead_id, call_datetime, shutdown_event
                )
                if result:
                    result['CallTime'] = call_datetime
                    result['Campaign'] = campaign
                    result['ClientID'] = client_id
                return result
            except (aiohttp.ClientConnectorError, aiohttp.ClientTimeout,
                    aiohttp.ServerTimeoutError, asyncio.TimeoutError) as e:
                logger.warning(f"Network error for lead {lead_id}: {type(e).__name__}")
                return None
            except aiohttp.ClientResponseError as e:
                if e.status == 429:  # Rate limited
                    logger.warning(f"Rate limited for lead {lead_id}")
                elif e.status in [500, 502, 503, 504]:  # Server errors
                    logger.warning(f"Server error {e.status} for lead {lead_id}")
                else:
                    logger.warning(f"HTTP error {e.status} for lead {lead_id}")
                return None
            except Exception as e:
                logger.debug(f"Error processing lead {lead_id}: {type(e).__name__} - {e}")
                return None

    # Create tasks for all leads in the batch
    tasks = [
        process_single_lead_safe(lead_id, call_datetime)
        for lead_id, call_datetime in lead_data_batch
    ]

    if not tasks:
        return []

    # Process all tasks concurrently but with limited concurrency
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")
        return []

    # Filter out exceptions and None results
    valid_records = []
    error_count = 0
    for result in results:
        if isinstance(result, Exception):
            error_count += 1
            continue
        if result:
            valid_records.append(result)

    if error_count > 0:
        logger.warning(f"Batch completed with {error_count} errors out of {len(tasks)} requests")

    return valid_records