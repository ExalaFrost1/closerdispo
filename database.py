"""
Database operations for BigQuery - STEP 1: Fix SQL Injection
"""
import os
import pandas as pd
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from google.cloud import bigquery
from typing import List, Dict, Any, Optional, AsyncGenerator
from utils import with_retry, get_today_date_ny, logger
from config import (
    SERVICE_ACCOUNT_PATH, PROJECT_ID, DATASET_ID,
    CREDENTIALS_TABLE, OUTPUT_TABLE, BATCH_SIZE
)


@asynccontextmanager
async def get_bigquery_client():
    """Context manager for BigQuery client with proper error handling"""
    client = None
    try:
        if not os.path.exists(SERVICE_ACCOUNT_PATH):
            raise FileNotFoundError(f"Service account file not found: {SERVICE_ACCOUNT_PATH}")

        client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)
        yield client
    except Exception as e:
        logger.error(f"Failed to create BigQuery client: {e}")
        raise
    finally:
        if client:
            try:
                client.close()
            except Exception as e:
                logger.warning(f"Error closing BigQuery client: {e}")


@with_retry()
async def fetch_credentials_chunked(client, offset=0, limit=100, shutdown_event=None):
    """Fetch credentials from BigQuery in chunks with retry logic - SECURE"""
    query = f"""
    SELECT Campaign, URL, Username, Password, ClientName, ClientID
    FROM `{PROJECT_ID}.{DATASET_ID}.{CREDENTIALS_TABLE}`
    ORDER BY Campaign, ClientID
    LIMIT @limit OFFSET @offset
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            bigquery.ScalarQueryParameter("offset", "INT64", offset)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    return results


@with_retry()
async def fetch_campaigns(client, shutdown_event=None) -> List[str]:
    """Fetch unique campaigns from BigQuery - SECURE"""
    query = f"""
    SELECT DISTINCT Campaign
    FROM `{PROJECT_ID}.{DATASET_ID}.{CREDENTIALS_TABLE}`
    WHERE Campaign IS NOT NULL
    ORDER BY Campaign
    """
    query_job = client.query(query)
    results = query_job.result()
    return [row.Campaign for row in results]


@with_retry()
async def fetch_credentials_by_campaign(client, campaign: str, shutdown_event=None):
    """Fetch credentials for a specific campaign - SECURE"""
    query = f"""
    SELECT Campaign, URL, Username, Password, ClientName, ClientID
    FROM `{PROJECT_ID}.{DATASET_ID}.{CREDENTIALS_TABLE}`
    WHERE Campaign = @campaign
    ORDER BY ClientID
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("campaign", "STRING", campaign)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    return list(results)


async def get_all_credentials_by_campaign(shutdown_event) -> AsyncGenerator[tuple, None]:
    """Get all credentials grouped by campaign"""
    try:
        async with get_bigquery_client() as client:
            campaigns = await fetch_campaigns(client, shutdown_event)

            for campaign in campaigns:
                if shutdown_event.is_set():
                    return

                credentials_list = await fetch_credentials_by_campaign(client, campaign, shutdown_event)
                if credentials_list:
                    yield campaign, credentials_list

    except Exception as e:
        logger.error(f"Critical error in get_all_credentials_by_campaign: {e}")
        raise


@with_retry()
async def check_existing_hash(client, hash_value: str, shutdown_event=None) -> Optional[Dict[str, Any]]:
    """Check if a hash already exists in TODAY'S BigQuery records - SECURE"""
    today = get_today_date_ny()
    tomorrow = today + timedelta(days=1)

    query = f"""
    SELECT LeadID, CallTime, ClientID, Campaign, Length, HashValue
    FROM `{PROJECT_ID}.{DATASET_ID}.{OUTPUT_TABLE}`
    WHERE HashValue = @hash_value
      AND CallTime >= @start_date
      AND CallTime < @end_date
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("hash_value", "STRING", hash_value),
            bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", today),
            bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", tomorrow)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    for row in results:
        return {
            'LeadID': row.LeadID,
            'CallTime': row.CallTime,
            'ClientID': row.ClientID,
            'Campaign': row.Campaign,
            'Length': row.Length,
            'HashValue': row.HashValue
        }
    return None


@with_retry()
async def update_record(client, hash_value: str, record_data: Dict[str, Any], shutdown_event=None) -> bool:
    """Update an existing record with new data - SECURE"""
    if record_data.get('Length') is None:
        return False

    query = f"""
    UPDATE `{PROJECT_ID}.{DATASET_ID}.{OUTPUT_TABLE}`
    SET 
        Status = @status,
        Phone = @phone,
        Ingroup = @ingroup,
        Length = @length
    WHERE HashValue = @hash_value
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", record_data['Status']),
            bigquery.ScalarQueryParameter("phone", "INT64", record_data['Phone'] if record_data['Phone'] is not None else None),
            bigquery.ScalarQueryParameter("ingroup", "STRING", record_data['Ingroup']),
            bigquery.ScalarQueryParameter("length", "INT64", record_data['Length'] if record_data['Length'] is not None else None),
            bigquery.ScalarQueryParameter("hash_value", "STRING", hash_value)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result()
    logger.info(f"Updated record with hash: {hash_value}")
    return True


@with_retry()
async def save_to_bigquery_batch(client, data_batch, shutdown_event=None):
    """Save a batch of records to BigQuery"""
    if not data_batch:
        return True

    # Create DataFrame for BigQuery
    bq_df = pd.DataFrame(data_batch)

    # Ensure proper data types
    if 'Phone' in bq_df.columns:
        bq_df['Phone'] = pd.to_numeric(bq_df['Phone'], errors='coerce').fillna(0).astype('int64')
        bq_df['Phone'] = bq_df['Phone'].replace(0, None)

    if 'Length' in bq_df.columns:
        bq_df['Length'] = pd.to_numeric(bq_df['Length'], errors='coerce').fillna(0).astype('int64')
        bq_df['Length'] = bq_df['Length'].replace(0, None)

    # Define the BigQuery table
    table_id = f'{PROJECT_ID}.{DATASET_ID}.{OUTPUT_TABLE}'

    # Load the DataFrame to BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )
    job = client.load_table_from_dataframe(bq_df, table_id, job_config=job_config)
    job.result()
    return True


@with_retry()
async def batch_check_existing_hashes_today(client, hash_values: List[str], shutdown_event=None) -> Dict[str, Dict]:
    """Check multiple hashes in a single query for TODAY'S records only - EFFICIENT & SECURE"""
    if not hash_values or shutdown_event.is_set():
        return {}

    try:
        today = get_today_date_ny()
        tomorrow = today + timedelta(days=1)

        # Create parameterized query for batch checking
        # For large batches, we might need to chunk this, but let's start simple
        if len(hash_values) > 1000:  # BigQuery parameter limit protection
            logger.warning(f"Large hash batch ({len(hash_values)}), processing in chunks")
            # Process in chunks of 1000
            all_results = {}
            for i in range(0, len(hash_values), 1000):
                chunk = hash_values[i:i + 1000]
                chunk_results = await batch_check_existing_hashes_today(client, chunk, shutdown_event)
                all_results.update(chunk_results)
            return all_results

        # Create parameters for the IN clause
        placeholders = ', '.join([f'@hash_{i}' for i in range(len(hash_values))])

        query = f"""
        SELECT LeadID, CallTime, ClientID, Campaign, Length, HashValue
        FROM `{PROJECT_ID}.{DATASET_ID}.{OUTPUT_TABLE}`
        WHERE HashValue IN ({placeholders})
          AND CallTime >= @start_date
          AND CallTime < @end_date
        """

        # Create parameters safely
        parameters = [
            bigquery.ScalarQueryParameter(f"hash_{i}", "STRING", hash_val)
            for i, hash_val in enumerate(hash_values)
        ]
        parameters.extend([
            bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", today),
            bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", tomorrow)
        ])

        job_config = bigquery.QueryJobConfig(query_parameters=parameters)
        query_job = client.query(query, job_config=job_config)
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

        logger.info(
            f"Batch hash check: {len(existing_records)} existing records found out of {len(hash_values)} checked")
        return existing_records

    except Exception as e:
        logger.error(f"Error in batch hash check: {e}")
        return {}