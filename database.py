"""
Database operations for BigQuery with Pub/Sub integration
"""
import os
import json
import asyncio
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from google.cloud import bigquery
from google.cloud import pubsub_v1
from typing import List, Dict, Any, Optional, AsyncGenerator
from utils import with_retry, get_today_date_ny, logger
from config import (
    SERVICE_ACCOUNT_PATH, PROJECT_ID, DATASET_ID,
    CREDENTIALS_TABLE, OUTPUT_TABLE, BATCH_SIZE
)


class PubSubPublisher:
    """Publisher for sending data to Pub/Sub topics"""

    def __init__(self, project_id: str, topic_name: str = "vicidial-closer-subscription"):
        self.project_id = project_id
        self.topic_name = topic_name
        self.publisher = None
        self.topic_path = None

    async def __aenter__(self):
        """Async context manager entry"""
        try:
            self.publisher = pubsub_v1.PublisherClient.from_service_account_json(SERVICE_ACCOUNT_PATH)
            self.topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
            logger.info(f"Pub/Sub publisher initialized for topic: {self.topic_name}")
            return self
        except Exception as e:
            logger.error(f"Failed to initialize Pub/Sub publisher: {e}")
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.publisher:
            # Close the publisher client
            try:
                self.publisher.stop()
            except Exception as e:
                logger.warning(f"Error stopping Pub/Sub publisher: {e}")

    @staticmethod
    def safe_str(value):
        """Safely convert value to string"""
        if value is None:
            return None
        return str(value)

    @staticmethod
    def safe_int(value):
        """Safely convert value to integer"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def safe_datetime_iso(value):
        """Safely convert datetime to ISO string"""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    async def publish_closer_disposition(self, record_data: Dict[str, Any]) -> bool:
        """Publish closer disposition data to Pub/Sub topic"""
        try:
            # Format data according to the schema
            data = {
                "CallTime": self.safe_datetime_iso(record_data.get('CallTime')),
                "Campaign": self.safe_str(record_data.get('Campaign')),
                "ClientName": self.safe_str(record_data.get('ClientName')),
                "ClientID": self.safe_str(record_data.get('ClientID')),
                "LeadID": self.safe_str(record_data.get('LeadID')),
                "Phone": self.safe_int(record_data.get('Phone')),
                "Status": self.safe_str(record_data.get('Status')),
                "Ingroup": self.safe_str(record_data.get('Ingroup')),
                "Length": self.safe_int(record_data.get('Length')),
                "HashValue": self.safe_str(record_data.get('HashValue'))
            }

            # Remove None values
            data = {k: v for k, v in data.items() if v is not None}

            message_data = json.dumps(data).encode("utf-8")

            # Publish message
            future = self.publisher.publish(self.topic_path, message_data)
            await asyncio.wrap_future(future)

            logger.debug(f"Published closer disposition record for LeadID: {record_data.get('LeadID')}")
            return True

        except Exception as e:
            logger.error(f"Failed to publish closer disposition record: {e}")
            logger.error(f"Record data: {record_data}")
            raise

    async def publish_batch(self, records: List[Dict[str, Any]]) -> int:
        """Publish a batch of records to Pub/Sub"""
        successful_count = 0

        for record in records:
            try:
                if await self.publish_closer_disposition(record):
                    successful_count += 1
            except Exception as e:
                logger.error(f"Failed to publish record in batch: {e}")
                # Continue with next record instead of failing entire batch
                continue

        logger.info(f"Published {successful_count}/{len(records)} records to Pub/Sub")
        return successful_count


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


@asynccontextmanager
async def get_pubsub_publisher(topic_name: str = "vicidial-closer-subscription"):
    """Context manager for Pub/Sub publisher"""
    async with PubSubPublisher(PROJECT_ID, topic_name) as publisher:
        yield publisher


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
async def update_record_pubsub(publisher: PubSubPublisher, hash_value: str, record_data: Dict[str, Any], shutdown_event=None) -> bool:
    """Send update record message to Pub/Sub - will be handled by subscriber"""
    if record_data.get('Length') is None:
        return False

    try:
        # Create update message with special marker
        update_data = {
            "UpdateType": "UPDATE_EXISTING",
            "HashValue": hash_value,
            "Status": record_data.get('Status'),
            "Phone": record_data.get('Phone'),
            "Ingroup": record_data.get('Ingroup'),
            "Length": record_data.get('Length'),
            "CallTime": publisher.safe_datetime_iso(record_data.get('CallTime')),
            "Campaign": record_data.get('Campaign'),
            "ClientID": record_data.get('ClientID'),
            "LeadID": record_data.get('Lead')
        }

        # Remove None values
        update_data = {k: v for k, v in update_data.items() if v is not None}

        message_data = json.dumps(update_data).encode("utf-8")
        future = publisher.publisher.publish(publisher.topic_path, message_data)
        await asyncio.wrap_future(future)

        logger.info(f"Sent update message for hash: {hash_value}")
        return True

    except Exception as e:
        logger.error(f"Failed to send update message: {e}")
        raise


@with_retry()
async def save_to_pubsub_batch(publisher: PubSubPublisher, data_batch: List[Dict[str, Any]], shutdown_event=None) -> bool:
    """Send batch of new records to Pub/Sub"""
    if not data_batch:
        return True

    try:
        successful_count = await publisher.publish_batch(data_batch)

        if successful_count == len(data_batch):
            logger.info(f"Successfully published all {len(data_batch)} records to Pub/Sub")
            return True
        else:
            logger.warning(f"Only published {successful_count}/{len(data_batch)} records to Pub/Sub")
            return successful_count > 0

    except Exception as e:
        logger.error(f"Failed to publish batch to Pub/Sub: {e}")
        raise


@with_retry()
async def batch_check_existing_hashes_today(client, hash_values: List[str], shutdown_event=None) -> Dict[str, Dict]:
    """Check multiple hashes in a single query for TODAY'S records only - EFFICIENT & SECURE"""
    if not hash_values or shutdown_event.is_set():
        return {}

    try:
        today = get_today_date_ny()
        tomorrow = today + timedelta(days=1)

        # Create parameterized query for batch checking
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


# Legacy function names for backward compatibility
async def update_record(client, hash_value: str, record_data: Dict[str, Any], shutdown_event=None) -> bool:
    """Legacy wrapper - now uses Pub/Sub via context manager"""
    async with get_pubsub_publisher() as publisher:
        return await update_record_pubsub(publisher, hash_value, record_data, shutdown_event)


async def save_to_bigquery_batch(client, data_batch, shutdown_event=None):
    """Legacy wrapper - now uses Pub/Sub via context manager"""
    async with get_pubsub_publisher() as publisher:
        return await save_to_pubsub_batch(publisher, data_batch, shutdown_event)