"""
Multiprocessing manager for running campaigns in parallel
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


def campaign_worker_function(campaign: str, credentials_data: List[Dict],
                            result_queue: Queue, shutdown_event: Event):
    """Worker function that runs in a separate process"""
    try:
        # Import inside the worker function to avoid pickling issues
        import asyncio
        import signal
        from campaign_processor import process_campaign
        from utils import logger

        # Set process title for easier identification
        try:
            import setproctitle
            setproctitle.setproctitle(f"vicidial-worker-{campaign}")
        except ImportError:
            pass

        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"Campaign {campaign} received signal {signum}")
            shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Create new event loop for this process
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Convert multiprocessing Event to asyncio Event
        async_shutdown_event = asyncio.Event()

        # Start a task to check the multiprocessing event
        async def check_shutdown():
            while not shutdown_event.is_set():
                await asyncio.sleep(0.1)
            async_shutdown_event.set()

        shutdown_task = loop.create_task(check_shutdown())

        try:
            # Convert dict data back to credential objects
            from types import SimpleNamespace
            credentials_list = [SimpleNamespace(**cred_dict) for cred_dict in credentials_data]

            # Run the campaign processing
            result = loop.run_until_complete(
                process_campaign(campaign, credentials_list, async_shutdown_event)
            )

            # Send result back to main process
            result_queue.put({
                'campaign': campaign,
                'processed_count': result,
                'status': 'completed',
                'error': None
            })

        except Exception as e:
            logger.error(f"Error in campaign {campaign}: {e}")
            result_queue.put({
                'campaign': campaign,
                'processed_count': 0,
                'status': 'error',
                'error': str(e)
            })
        finally:
            shutdown_task.cancel()
            try:
                # Cancel any remaining tasks
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            finally:
                loop.close()

    except Exception as e:
        logger.error(f"Critical error in worker for campaign {campaign}: {e}")
        try:
            result_queue.put({
                'campaign': campaign,
                'processed_count': 0,
                'status': 'critical_error',
                'error': str(e)
            })
        except Exception:
            pass


class CampaignWorker:
    """Worker class for processing campaigns in separate processes"""

    def __init__(self, campaign: str, credentials_data: List[Dict],
                 result_queue: Queue, shutdown_event: Event):
        self.campaign = campaign
        self.credentials_data = credentials_data  # Store as dicts instead of objects
        self.result_queue = result_queue
        self.shutdown_event = shutdown_event
        self.process = None

    def start(self):
        """Start the worker process"""
        self.process = Process(
            target=campaign_worker_function,
            args=(self.campaign, self.credentials_data, self.result_queue, self.shutdown_event)
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


class MultiprocessCampaignManager:
    """Manager for running multiple campaigns in parallel processes"""

    def __init__(self, max_processes: int = MAX_CAMPAIGN_PROCESSES):
        self.max_processes = max_processes
        self.active_workers: List[CampaignWorker] = []
        self.result_queue = Queue()
        self.shutdown_event = Event()
        self.results: List[Dict[str, Any]] = []

        # Set multiprocessing start method for Windows compatibility
        if os.name == 'nt':  # Windows
            try:
                mp.set_start_method('spawn', force=True)
            except RuntimeError:
                pass  # Already set

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals for the main process"""
        logger.info(f"Multiprocess manager received signal {signum}. Setting shutdown event...")
        self.shutdown_event.set()
        # Don't call sys.exit() immediately - let the cleanup happen naturally

    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown"""
        # Don't set up signal handlers here - let the main script handle them
        pass

    def _cleanup_workers(self):
        """Clean up all active workers"""
        logger.info("Cleaning up active workers...")
        for worker in self.active_workers:
            try:
                worker.terminate()
            except Exception as e:
                logger.error(f"Error terminating worker for campaign {worker.campaign}: {e}")
        self.active_workers.clear()

    def _collect_results(self, timeout: float = 1.0) -> List[Dict[str, Any]]:
        """Collect results from completed processes"""
        collected_results = []

        try:
            while True:
                try:
                    result = self.result_queue.get_nowait()
                    collected_results.append(result)
                    logger.info(f"Collected result for campaign {result['campaign']}: "
                              f"Status={result['status']}, Processed={result['processed_count']}")
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
            else:
                logger.info(f"Worker for campaign {worker.campaign} has completed")
        self.active_workers = active_workers

    async def process_all_campaigns(self) -> Dict[str, Any]:
        """Process all campaigns using multiprocessing"""
        # Don't set up signal handlers in the multiprocess manager

        start_time = datetime.now(NY_TZ)
        logger.info(f"Starting multiprocess campaign processing at {start_time.strftime('%H:%M:%S')}")

        try:
            # Get all campaigns and their credentials
            campaigns_data = []
            async for campaign, credentials_list in get_all_credentials_by_campaign(self.shutdown_event):
                if self.shutdown_event.is_set():
                    break

                # Convert credentials to dictionaries to avoid pickling issues
                credentials_data = []
                for cred in credentials_list:
                    credentials_data.append({
                        'Campaign': cred.Campaign,
                        'URL': cred.URL,
                        'Username': cred.Username,
                        'Password': cred.Password,
                        'ClientName': cred.ClientName,
                        'ClientID': cred.ClientID
                    })

                campaigns_data.append((campaign, credentials_data))

            if not campaigns_data:
                logger.warning("No campaigns found to process")
                return {'total_campaigns': 0, 'total_processed': 0, 'results': []}

            logger.info(f"Found {len(campaigns_data)} campaigns to process")

            # Process campaigns in batches to respect max_processes limit
            pending_campaigns = campaigns_data[:]

            while pending_campaigns or self.active_workers:
                if self.shutdown_event.is_set():
                    break

                # Start new workers if we have capacity and pending campaigns
                while (len(self.active_workers) < self.max_processes and
                       pending_campaigns and not self.shutdown_event.is_set()):

                    campaign, credentials_data = pending_campaigns.pop(0)

                    logger.info(f"Starting worker for campaign: {campaign} "
                              f"({len(self.active_workers) + 1}/{self.max_processes})")

                    worker = CampaignWorker(
                        campaign, credentials_data,
                        self.result_queue, self.shutdown_event
                    )
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

            # Wait for any remaining processes to complete
            remaining_workers = [w for w in self.active_workers if w.is_alive()]
            if remaining_workers:
                logger.info(f"Waiting for {len(remaining_workers)} remaining workers to complete...")
                for worker in remaining_workers:
                    worker.process.join(timeout=30)  # Wait up to 30 seconds per worker
                    if worker.is_alive():
                        logger.warning(f"Worker for campaign {worker.campaign} didn't finish, terminating...")
                        worker.terminate()

            end_time = datetime.now(NY_TZ)
            total_duration = (end_time - start_time).total_seconds()
            total_processed = sum(r['processed_count'] for r in self.results)

            logger.info(f"Multiprocess campaign processing completed in {total_duration:.1f} seconds")
            logger.info(f"Total campaigns: {len(self.results)}, Total processed: {total_processed}")

            return {
                'total_campaigns': len(self.results),
                'total_processed': total_processed,
                'duration_seconds': total_duration,
                'results': self.results
            }

        except Exception as e:
            logger.error(f"Error in multiprocess campaign manager: {e}")
            return {'total_campaigns': 0, 'total_processed': 0, 'results': [], 'error': str(e)}

        finally:
            self._cleanup_workers()


# Function to be called from main script
async def process_campaigns_multiprocess(max_processes: int = MAX_CAMPAIGN_PROCESSES) -> Dict[str, Any]:
    """Main function to process campaigns using multiprocessing"""
    manager = MultiprocessCampaignManager(max_processes)
    return await manager.process_all_campaigns()