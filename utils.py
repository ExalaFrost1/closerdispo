"""
Utility functions for the Vicidial data processor
"""
import os
import tempfile
import threading
import logging
import random
import re
import hashlib
import uuid
from datetime import datetime, timedelta
from urllib.parse import urlparse
from functools import wraps
import asyncio
import pandas as pd
from config import NY_TZ, MAX_RETRIES, RETRY_DELAY

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SensitiveDataFilter(logging.Filter):
    """Filter to remove sensitive information from logs"""

    def filter(self, record):
        if hasattr(record, 'msg'):
            msg = str(record.msg)
            msg = re.sub(r'(password=)[^&\s]+', r'\1***MASKED***', msg)
            msg = re.sub(r'(pwd=)[^&\s]+', r'\1***MASKED***', msg)
            msg = re.sub(r'(auth=)[^,\s\)]+', r'\1***MASKED***', msg)
            msg = re.sub(r'(password["\']?\s*[:=]\s*["\']?)[^,\s\)}"\']+',
                         r'\1***MASKED***', msg, flags=re.IGNORECASE)
            record.msg = msg
        return True


# Add filter to logger
logger.addFilter(SensitiveDataFilter())


class TempFileManager:
    """Thread-safe temporary file manager"""

    def __init__(self):
        self.temp_files = set()
        self.lock = threading.Lock()

    def add_temp_file(self, filepath):
        """Add file to tracking"""
        with self.lock:
            self.temp_files.add(filepath)

    def remove_temp_file(self, filepath):
        """Remove file from tracking"""
        with self.lock:
            self.temp_files.discard(filepath)

    def cleanup_temp_files(self):
        """Clean up all tracked temporary files"""
        with self.lock:
            files_to_clean = self.temp_files.copy()

        for filepath in files_to_clean:
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
                    logger.info(f"Cleaned up temporary file: {os.path.basename(filepath)}")
            except Exception as e:
                logger.error(f"Error cleaning up temp file {os.path.basename(filepath)}: {e}")

        with self.lock:
            self.temp_files.clear()


# Global temp file manager
temp_file_manager = TempFileManager()


def with_retry(max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """Decorator for automatic retry with exponential backoff"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            shutdown_event = None
            if args and hasattr(args[0], 'shutdown_event'):
                shutdown_event = args[0].shutdown_event
            elif 'shutdown_event' in kwargs:
                shutdown_event = kwargs['shutdown_event']

            for attempt in range(max_retries):
                try:
                    if shutdown_event and shutdown_event.is_set():
                        raise KeyboardInterrupt("Shutdown requested")
                    return await func(*args, **kwargs)
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Function {func.__name__} failed after {max_retries} attempts: {e}")
                        raise
                    wait_time = delay * (2 ** attempt)
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
            return None

        return wrapper

    return decorator


def with_adaptive_retry(max_retries=3, base_delay=5):
    """Decorator for adaptive retry with exponential backoff and jitter"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            shutdown_event = None
            if args and hasattr(args[0], 'shutdown_event'):
                shutdown_event = args[0].shutdown_event
            elif 'shutdown_event' in kwargs:
                shutdown_event = kwargs['shutdown_event']

            last_exception = None

            for attempt in range(max_retries + 1):  # +1 for initial attempt
                try:
                    if shutdown_event and shutdown_event.is_set():
                        raise KeyboardInterrupt("Shutdown requested")
                    return await func(*args, **kwargs)

                except KeyboardInterrupt:
                    raise

                except (aiohttp.ClientConnectorError, aiohttp.ClientTimeout,
                        aiohttp.ServerTimeoutError, asyncio.TimeoutError,
                        ConnectionResetError, OSError) as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(
                            f"Network error in {func.__name__} after {max_retries + 1} attempts: {type(e).__name__}")
                        raise

                    # Adaptive delay: longer for network issues
                    wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(
                        f"Network error in {func.__name__} (attempt {attempt + 1}): {type(e).__name__}. Retrying in {wait_time:.1f}s...")
                    await asyncio.sleep(wait_time)

                except aiohttp.ClientResponseError as e:
                    last_exception = e
                    if e.status == 429:  # Rate limited
                        if attempt == max_retries:
                            logger.error(f"Rate limited in {func.__name__} after {max_retries + 1} attempts")
                            raise
                        # Longer wait for rate limiting
                        wait_time = base_delay * (3 ** attempt) + random.uniform(1, 3)
                        logger.warning(
                            f"Rate limited in {func.__name__} (attempt {attempt + 1}). Retrying in {wait_time:.1f}s...")
                        await asyncio.sleep(wait_time)
                    elif e.status in [500, 502, 503, 504]:  # Server errors
                        if attempt == max_retries:
                            logger.error(f"Server error {e.status} in {func.__name__} after {max_retries + 1} attempts")
                            raise
                        wait_time = base_delay * (2 ** attempt) + random.uniform(0, 2)
                        logger.warning(
                            f"Server error {e.status} in {func.__name__} (attempt {attempt + 1}). Retrying in {wait_time:.1f}s...")
                        await asyncio.sleep(wait_time)
                    else:
                        # Don't retry for other HTTP errors (like 404, 401, etc.)
                        raise

                except Exception as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(
                            f"Error in {func.__name__} after {max_retries + 1} attempts: {type(e).__name__} - {e}")
                        raise
                    wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(
                        f"Error in {func.__name__} (attempt {attempt + 1}): {type(e).__name__}. Retrying in {wait_time:.1f}s...")
                    await asyncio.sleep(wait_time)

            # This shouldn't be reached, but just in case
            if last_exception:
                raise last_exception
            return None

        return wrapper

    return decorator

def generate_hash(lead_id: str, call_datetime: datetime, client_id: str, campaign: str) -> str:
    """Generate a unique hash for the record"""
    hash_string = f"{lead_id}_{call_datetime.isoformat()}_{client_id}_{campaign}"
    return hashlib.md5(hash_string.encode('utf-8')).hexdigest()


def get_today_date_ny() -> datetime:
    """Get today's date in NY timezone at midnight"""
    now = datetime.now(NY_TZ)
    return datetime(now.year, now.month, now.day, tzinfo=NY_TZ)


def clean_lead_id(lead_id):
    """Clean and validate lead ID"""
    if pd.isna(lead_id):
        return None
    lead_id_str = str(lead_id).strip()
    if not lead_id_str or lead_id_str == '0':
        return None
    return lead_id_str.split('.')[0]


def get_lead_details_url(csv_url):
    """Construct lead details URL from CSV URL"""
    try:
        parsed_url = urlparse(csv_url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path.split('user_stats.php')[0]}"
        return f"{base_url}admin_modify_lead.php?lead_id={{lead_id}}&archive_search=No&archive_log=0"
    except Exception as e:
        logger.error(f"Error constructing lead details URL: {e}")
        raise


def update_csv_url_with_today_date(original_url):
    """Update the CSV URL with today's date in New York timezone"""
    try:
        today = datetime.now(NY_TZ).strftime("%Y-%m-%d")
        updated_url = re.sub(r'begin_date=\d{4}-\d{2}-\d{2}', f'begin_date={today}', original_url)
        updated_url = re.sub(r'end_date=\d{4}-\d{2}-\d{2}', f'end_date={today}', updated_url)
        return updated_url
    except Exception as e:
        logger.error(f"Error updating CSV URL with today's date: {e}")
        return original_url


def create_temp_file(prefix="vicidial_export_", suffix=".csv"):
    """Create a temporary file and return path and file descriptor"""
    file_id = str(uuid.uuid4())[:8]
    temp_fd, temp_path = tempfile.mkstemp(suffix=suffix, prefix=f'{prefix}{file_id}_')
    temp_file_manager.add_temp_file(temp_path)
    return temp_fd, temp_path, file_id