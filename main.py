#!/usr/bin/env python3
"""
Unified main script with configurable parallelization strategies
"""
import asyncio
import warnings
import sys
import os
import signal
import atexit
from datetime import datetime, timedelta

# Windows compatibility setup
if os.name == 'nt':  # Windows
    import multiprocessing as mp

    if __name__ == '__main__':
        mp.freeze_support()
        try:
            mp.set_start_method('spawn', force=True)
        except RuntimeError:
            pass

from utils import logger, temp_file_manager
from config import get_cycle_interval_minutes, NY_TZ, PARALLELIZATION_LEVEL

# Import both multiprocessing approaches
from multiprocess_manager import process_campaigns_multiprocess
from enhanced_multiprocess import process_clients_multiprocess

# Disable SSL-related warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')
atexit.register(temp_file_manager.cleanup_temp_files)


def print_processing_summary(results):
    """Print a summary of processing results"""
    logger.info("=" * 60)
    logger.info("PROCESSING SUMMARY")
    logger.info("=" * 60)

    if 'error' in results:
        logger.error(f"Processing failed with error: {results['error']}")
        return

    # Handle both campaign-level and client-level results
    if 'total_clients' in results:
        # Client-level parallelization results
        logger.info(f"Parallelization: Client-level")
        logger.info(f"Total campaigns: {results['total_campaigns']}")
        logger.info(f"Total clients processed: {results['total_clients']}")
        logger.info(f"Total records processed: {results['total_processed']}")
        logger.info(f"Processing duration: {results.get('duration_seconds', 0):.1f} seconds")

        if results.get('campaign_results'):
            logger.info("\nCampaign breakdown:")
            for campaign, data in results['campaign_results'].items():
                logger.info(f"  📁 {campaign}: {data['processed']} records from {data['count']} clients")
                for client in data['clients']:
                    status_symbol = "✓" if client['status'] == 'completed' else "✗"
                    logger.info(
                        f"    {status_symbol} Client {client['client_id']}: {client['processed_count']} records")
    else:
        # Campaign-level parallelization results
        logger.info(f"Parallelization: Campaign-level")
        logger.info(f"Total campaigns processed: {results['total_campaigns']}")
        logger.info(f"Total records processed: {results['total_processed']}")
        logger.info(f"Processing duration: {results.get('duration_seconds', 0):.1f} seconds")

        if results.get('results'):
            logger.info("\nCampaign breakdown:")
            for result in results['results']:
                status_symbol = "✓" if result['status'] == 'completed' else "✗"
                logger.info(f"  {status_symbol} {result['campaign']}: {result['processed_count']} records "
                            f"({result['status']})")
                if result.get('error'):
                    logger.error(f"    Error: {result['error']}")

    logger.info("=" * 60)
    logger.info("CYCLE WILL CONTINUE - Waiting for next scheduled run...")
    logger.info("=" * 60)


async def main_loop():
    """Main processing loop with configurable parallelization"""
    cycle_interval = get_cycle_interval_minutes()

    logger.info(f"Multiprocess Vicidial Processor started")
    logger.info(f"Parallelization level: {PARALLELIZATION_LEVEL}")
    logger.info(f"Cycle interval: {cycle_interval} minutes")

    if PARALLELIZATION_LEVEL == "client":
        logger.info("🚀 Using CLIENT-LEVEL parallelization - Each client runs in its own process")
        logger.info("   This means ALL client IDs across ALL campaigns run simultaneously")
    else:
        logger.info("📁 Using CAMPAIGN-LEVEL parallelization - Each campaign runs in its own process")
        logger.info("   This means clients within the same campaign run sequentially")

    # Create shutdown event
    shutdown_event = asyncio.Event()

    # Set up signal handlers
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}. Setting shutdown event...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    cycle_number = 1

    try:
        while not shutdown_event.is_set():
            cycle_start_time = datetime.now(NY_TZ)
            next_run_time = cycle_start_time + timedelta(minutes=cycle_interval)

            logger.info(f"Starting processing cycle #{cycle_number} at {cycle_start_time.strftime('%H:%M:%S')}")

            try:
                # Choose processing approach based on configuration
                if PARALLELIZATION_LEVEL == "client":
                    results = await process_clients_multiprocess()
                else:
                    results = await process_campaigns_multiprocess()

                if not shutdown_event.is_set():
                    cycle_end_time = datetime.now(NY_TZ)
                    processing_duration = (cycle_end_time - cycle_start_time).total_seconds()

                    # Print processing summary
                    print_processing_summary(results)

                    logger.info(f"Cycle #{cycle_number} completed in {processing_duration:.1f} seconds")

                    # Calculate sleep time until next cycle
                    now = datetime.now(NY_TZ)
                    if now < next_run_time:
                        sleep_duration = (next_run_time - now).total_seconds()
                        logger.info(
                            f"Next cycle #{cycle_number + 1} scheduled at {next_run_time.strftime('%H:%M:%S')}. "
                            f"Sleeping for {sleep_duration:.1f} seconds...")

                        try:
                            await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_duration)
                            logger.info("Shutdown event was set during sleep period")
                            break
                        except asyncio.TimeoutError:
                            logger.info(f"Sleep timeout reached. Starting cycle #{cycle_number + 1}...")
                            cycle_number += 1
                            continue
                    else:
                        logger.warning(f"Processing took longer than {cycle_interval} minutes. "
                                       "Starting next cycle immediately.")
                        cycle_number += 1
                        continue

            except Exception as e:
                logger.error(f"Unexpected error in main loop cycle #{cycle_number}: {e}")
                if not shutdown_event.is_set():
                    logger.info("Restarting processing cycle in 10 seconds...")
                    try:
                        await asyncio.wait_for(shutdown_event.wait(), timeout=10)
                        logger.info("Shutdown requested during error recovery")
                        break
                    except asyncio.TimeoutError:
                        logger.info("Error recovery completed. Continuing with next cycle...")
                        cycle_number += 1
                        continue

    except Exception as e:
        logger.error(f"Critical error in main loop: {e}")
    finally:
        logger.info(f"Main loop ended gracefully after {cycle_number} cycles")
        temp_file_manager.cleanup_temp_files()


def run_single_cycle():
    """Run a single processing cycle"""

    async def single_cycle():
        logger.info("Running single processing cycle...")
        logger.info(f"Parallelization level: {PARALLELIZATION_LEVEL}")

        if PARALLELIZATION_LEVEL == "client":
            results = await process_clients_multiprocess()
        else:
            results = await process_campaigns_multiprocess()

        print_processing_summary(results)
        return results

    return asyncio.run(single_cycle())


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "--single":
            # Run single cycle for testing
            results = run_single_cycle()
            total_processed = results.get('total_processed', 0)
            sys.exit(0 if total_processed >= 0 else 1)
        else:
            # Run continuous loop
            asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Script stopped by user")
    except Exception as e:
        logger.error(f"Critical error: {e}")
        sys.exit(1)
    finally:
        temp_file_manager.cleanup_temp_files()
        logger.info("Script cleanup completed")