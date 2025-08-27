"""
Configuration module for the Vicidial data processor
"""
import os
from datetime import timedelta
from pytz import timezone

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds
NY_TZ = timezone('America/New_York')
BATCH_SIZE = 50  # Number of records to process at once
CSV_CHUNKSIZE = 50000  # Number of rows to read at once from CSV
LEAD_BATCH_SIZE = 50
DB_BATCH_SIZE = 100  # Number of records to write to DB at once
TIME_TOLERANCE = 3  # Seconds tolerance for date matching
CYCLE_INTERVAL_MINUTES = 60  # Run every 15 minutes

# For testing - you can set this to a smaller value
TEST_MODE = False  # Set to True for faster testing cycles
TEST_INTERVAL_MINUTES = 2  # Use this interval when TEST_MODE is True

# Get the actual interval to use
def get_cycle_interval_minutes():
    """Get the cycle interval based on test mode"""
    return TEST_INTERVAL_MINUTES if TEST_MODE else CYCLE_INTERVAL_MINUTES

# Multiprocessing settings
MAX_CAMPAIGN_PROCESSES = 2  # Maximum number of campaign processes to run in parallel
PROCESS_TIMEOUT = 3600  # 1 hour timeout for each process
PARALLELIZATION_LEVEL = "client"
MAX_CLIENT_PROCESSES = 6  # Increased for handling hundreds of CIDs

# BigQuery settings
SERVICE_ACCOUNT_PATH = os.environ.get(
    'GOOGLE_APPLICATION_CREDENTIALS',
    '/home/vicimanager/CloserDispo/XFERCloserDataKey.json'
)
PROJECT_ID = 'inflection-403908'
DATASET_ID = 'confinality_vicidial'
CREDENTIALS_TABLE = 'CloserData'
OUTPUT_TABLE = 'XferCloserDisposition'

# HTTP settings - OPTIMIZED
CONCURRENT_REQUESTS = 20   # Increased from 10
HTTP_TIMEOUT = 300         # Reduced from 300
CONNECT_TIMEOUT = 30       # Reduced from 30
MAX_CONNECTIONS_PER_HOST = 8  # New setting

# Performance monitoring
ENABLE_PERFORMANCE_LOGGING = True
LOG_BATCH_PROGRESS = True  # Log every 10th batch instead of every batch

# Memory optimization
MEMORY_EFFICIENT_MODE = True  # Enable memory optimizations for large datasets
CLEANUP_FREQUENCY = 100  # Clean up temp files every N batches

# Add rate limiting
REQUEST_DELAY = 0.2
