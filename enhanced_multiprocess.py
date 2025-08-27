"""
Enhanced multiprocessing manager that runs individual clients in parallel - FIXED VERSION
"""
import asyncio
import multiprocessing as mp
from multiprocessing import Process, Queue, Event
from typing import List, Dict, Any, Tuple
import signal
import sys
import os
from datetime import datetime

from utils import logger, temp_file_manager
from database import get_all_credentials_by_campaign
from config import MAX_CAMPAIGN_PROCESSES, PROCESS_TIMEOUT, NY_TZ


def client_worker_function(client_data: Dict, result_queue: Queue, shutdown_event: Event):
    """Worker function that processes a single client in a separate process - FIXED"""
    client_id = client_data.get('ClientID', 'unknown')
    campaign = client_data.get('Campaign', 'unknown')

    try:
        # Set process title for easier identification
        try:
            import setproctitle
            setproctitle.setproctitle(f"vicidial-client-{campaign}-{client_id}")
        except ImportError:
            pass

        # Set up signal handlers
        def signal_handler(signum, frame):
            logger.info(f"Client {client_id} received signal {signum}")
            shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # CRITICAL FIX: Create and run everything in a single async function
        async def run_client_processing():
            """Main async function that runs the entire client processing"""
            try:
                # Import inside the async function to avoid import issues
                from campaign_processor import process_client_credentials
                from database import get_bigquery_client
                import aiohttp
                from config import HTTP_TIMEOUT, CONCURRENT_REQUESTS

                # Convert dict back to credential object
                from types import SimpleNamespace
                credentials = SimpleNamespace(**client_data)

                # Create async shutdown event
                async_shutdown_event = asyncio.Event()

                # Create a task to monitor the multiprocessing event
                async def monitor_shutdown():
                    while not shutdown_event.is_set():
                        await asyncio.sleep(0.1)
                    async_shutdown_event.set()

                monitor_task = asyncio.create_task(monitor_shutdown())

                try:
                    # Create HTTP session and BigQuery client within the async context
                    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
                    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS, limit_per_host=5)

                    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                        async with get_bigquery_client() as bq_client:
                            result = await process_client_credentials(
                                session, bq_client, credentials, async_shutdown_event
                            )
                            return result

                finally:
                    monitor_task.cancel()
                    try:
                        await monitor_task
                    except asyncio.CancelledError:
                        pass

            except Exception as e:
                logger.error(f"Error in client processing for {client_id}: {e}")
                raise

        # CRITICAL FIX: Use asyncio.run() to properly set up the event loop
        try:
            result = asyncio.run(run_client_processing())

            # Send success result back to main process
            result_queue.put({
                'client_id': client_id,
                'campaign': campaign,
                'processed_count': result,
                'status': 'completed',
                'error': None
            })

        except Exception as e:
            logger.error(f"Error running async processing for client {client_id}: {e}")
            result_queue.put({
                'client_id': client_id,
                'campaign': campaign,
                'processed_count': 0,
                'status': 'error',
                'error': str(e)
            })

    except Exception as e:
        logger.error(f"Critical error in client worker for {client_id}: {e}")
        try:
            result_queue.put({
                'client_id': client_id,
                'campaign': campaign,
                'processed_count': 0,
                'status': 'critical_error',
                'error': str(e)
            })
        except Exception:
            pass


class ClientWorker:
    """Worker class for processing individual clients in separate processes"""

    def __init__(self, client_data: Dict, result_queue: Queue, shutdown_event: Event):
        self.client_data = client_data
        self.result_queue = result_queue
        self.shutdown_event = shutdown_event
        self.process = None

    def start(self):
        """Start the worker process"""
        self.process = Process(
            target=client_worker_function,
            args=(self.client_data, self.result_queue, self.shutdown_event)
        )
        self.process.start()
        return self.process

    def is_alive(self):
        """Check if the worker process is alive"""
        return self.process and self.process.is_alive()

    def terminate(self):
        """Terminate the worker process"""
        if self.process and self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=5)
            if self.process.is_alive():
                self.process.kill()


class EnhancedMultiprocessManager:
    """Manager for running individual clients in parallel processes"""

    def __init__(self, max_processes: int = None):
        # Use more processes for client-level parallelization
        self.max_processes = max_processes or (MAX_CAMPAIGN_PROCESSES * 3)  # e.g., 12 processes
        self.active_workers: List[ClientWorker] = []
        self.result_queue = Queue()
        self.shutdown_event = Event()
        self.results: List[Dict[str, Any]] = []

        # Set multiprocessing start method for Windows compatibility
        if os.name == 'nt':  # Windows
            try:
                mp.set_start_method('spawn', force=True)
            except RuntimeError:
                pass

    def _cleanup_workers(self):
        """Clean up all active workers"""
        logger.info("Cleaning up active workers...")
        for worker in self.active_workers:
            try:
                worker.terminate()
            except Exception as e:
                logger.error(f"Error terminating worker: {e}")
        self.active_workers.clear()

    def _collect_results(self) -> List[Dict[str, Any]]:
        """Collect results from completed processes"""
        collected_results = []

        try:
            while True:
                try:
                    result = self.result_queue.get_nowait()
                    collected_results.append(result)
                    logger.info(f"Collected result for client {result['client_id']} in campaign {result['campaign']}: "
                                f"Status={result['status']}, Processed={result['processed_count']}")

                    # Log errors for debugging
                    if result['status'] == 'error' and result.get('error'):
                        logger.error(f"Client {result['client_id']} error details: {result['error']}")

                except:
                    break
        except Exception as e:
            logger.error(f"Error collecting results: {e}")

        return collected_results

    def _remove_completed_workers(self):
        """Remove completed workers from active list"""
        active_workers = []
        for worker in self.active_workers:
            if worker.is_alive():
                active_workers.append(worker)
        self.active_workers = active_workers

    async def process_all_clients(self) -> Dict[str, Any]:
        """Process all clients using individual process per client"""
        start_time = datetime.now(NY_TZ)
        logger.info(f"Starting enhanced multiprocess processing at {start_time.strftime('%H:%M:%S')}")
        logger.info(f"Maximum parallel processes: {self.max_processes}")

        try:
            # Get all clients from all campaigns
            all_clients = []
            campaign_counts = {}

            async for campaign, credentials_list in get_all_credentials_by_campaign(self.shutdown_event):
                if self.shutdown_event.is_set():
                    break

                campaign_counts[campaign] = len(credentials_list)

                # Convert each credential to dictionary
                for cred in credentials_list:
                    client_data = {
                        'Campaign': cred.Campaign,
                        'URL': cred.URL,
                        'Username': cred.Username,
                        'Password': cred.Password,
                        'ClientName': cred.ClientName,
                        'ClientID': cred.ClientID
                    }
                    all_clients.append(client_data)

            if not all_clients:
                logger.warning("No clients found to process")
                return {'total_campaigns': 0, 'total_clients': 0, 'total_processed': 0, 'results': []}

            logger.info(f"Found {len(all_clients)} total clients across {len(campaign_counts)} campaigns:")
            for campaign, count in campaign_counts.items():
                logger.info(f"  - {campaign}: {count} clients")

            # Process clients in batches to respect max_processes limit
            pending_clients = all_clients[:]

            while pending_clients or self.active_workers:
                if self.shutdown_event.is_set():
                    break

                # Start new workers if we have capacity and pending clients
                while (len(self.active_workers) < self.max_processes and
                       pending_clients and not self.shutdown_event.is_set()):
                    client_data = pending_clients.pop(0)
                    client_id = client_data['ClientID']
                    campaign = client_data['Campaign']

                    logger.info(f"Starting worker for client {client_id} in campaign {campaign} "
                                f"({len(self.active_workers) + 1}/{self.max_processes})")

                    worker = ClientWorker(client_data, self.result_queue, self.shutdown_event)
                    worker.start()
                    self.active_workers.append(worker)

                # Collect results and remove completed workers
                new_results = self._collect_results()
                self.results.extend(new_results)
                self._remove_completed_workers()

                # Small delay to prevent busy waiting
                await asyncio.sleep(2)

            # Final result collection
            final_results = self._collect_results()
            self.results.extend(final_results)

            # Wait for any remaining processes
            remaining_workers = [w for w in self.active_workers if w.is_alive()]
            if remaining_workers:
                logger.info(f"Waiting for {len(remaining_workers)} remaining workers...")
                for worker in remaining_workers:
                    worker.process.join(timeout=30)
                    if worker.is_alive():
                        worker.terminate()

            end_time = datetime.now(NY_TZ)
            total_duration = (end_time - start_time).total_seconds()
            total_processed = sum(r['processed_count'] for r in self.results)

            # Group results by campaign for summary
            campaign_results = {}
            for result in self.results:
                campaign = result['campaign']
                if campaign not in campaign_results:
                    campaign_results[campaign] = {'count': 0, 'processed': 0, 'clients': []}
                campaign_results[campaign]['count'] += 1
                campaign_results[campaign]['processed'] += result['processed_count']
                campaign_results[campaign]['clients'].append({
                    'client_id': result['client_id'],
                    'processed_count': result['processed_count'],
                    'status': result['status']
                })

            logger.info(f"Enhanced multiprocess processing completed in {total_duration:.1f} seconds")
            logger.info(f"Total clients: {len(self.results)}, Total processed: {total_processed}")

            return {
                'total_campaigns': len(campaign_results),
                'total_clients': len(self.results),
                'total_processed': total_processed,
                'duration_seconds': total_duration,
                'campaign_results': campaign_results,
                'results': self.results
            }

        except Exception as e:
            logger.error(f"Error in enhanced multiprocess manager: {e}")
            return {'total_campaigns': 0, 'total_clients': 0, 'total_processed': 0, 'results': [], 'error': str(e)}

        finally:
            self._cleanup_workers()


# Function to be called from main script
async def process_clients_multiprocess(max_processes: int = None) -> Dict[str, Any]:
    """Main function to process individual clients using multiprocessing"""
    manager = EnhancedMultiprocessManager(max_processes)
    return await manager.process_all_clients()