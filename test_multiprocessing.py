#!/usr/bin/env python3
"""
Test script for debugging multiprocessing issues
"""
import os
import sys
import asyncio
import multiprocessing as mp
from datetime import datetime

# Windows compatibility
if os.name == 'nt' and __name__ == '__main__':
    mp.freeze_support()
    try:
        mp.set_start_method('spawn', force=True)
    except RuntimeError:
        pass

from utils import logger
from database import get_all_credentials_by_campaign


def simple_worker(campaign_name, credentials_data, result_queue):
    """Simple worker function for testing"""
    try:
        logger.info(f"Worker started for campaign: {campaign_name}")
        logger.info(f"Credentials count: {len(credentials_data)}")

        # Simulate some work
        import time
        time.sleep(2)

        result_queue.put({
            'campaign': campaign_name,
            'status': 'success',
            'credentials_count': len(credentials_data)
        })

    except Exception as e:
        logger.error(f"Error in worker for {campaign_name}: {e}")
        result_queue.put({
            'campaign': campaign_name,
            'status': 'error',
            'error': str(e)
        })


async def test_data_fetching():
    """Test fetching data from BigQuery"""
    logger.info("Testing data fetching from BigQuery...")

    shutdown_event = asyncio.Event()
    campaigns_found = 0

    try:
        async for campaign, credentials_list in get_all_credentials_by_campaign(shutdown_event):
            campaigns_found += 1
            logger.info(f"Found campaign: {campaign} with {len(credentials_list)} credentials")

            # Test converting to dict (what we need for multiprocessing)
            credentials_data = []
            for cred in credentials_list:
                try:
                    cred_dict = {
                        'Campaign': cred.Campaign,
                        'URL': cred.URL,
                        'Username': cred.Username,
                        'Password': '***MASKED***',  # Don't log real password
                        'ClientName': cred.ClientName,
                        'ClientID': cred.ClientID
                    }
                    credentials_data.append(cred_dict)
                except Exception as e:
                    logger.error(f"Error converting credential to dict: {e}")

            logger.info(f"Successfully converted {len(credentials_data)} credentials to dict format")

            # Only test first campaign to avoid too much output
            if campaigns_found >= 2:
                break

    except Exception as e:
        logger.error(f"Error fetching campaign data: {e}")
        return False

    logger.info(f"Data fetching test completed. Found {campaigns_found} campaigns.")
    return campaigns_found > 0


def test_simple_multiprocessing():
    """Test simple multiprocessing without BigQuery objects"""
    logger.info("Testing simple multiprocessing...")

    # Test data
    test_campaigns = [
        ('test_campaign_1', [{'ClientID': '001', 'ClientName': 'Test Client 1'}]),
        ('test_campaign_2', [{'ClientID': '002', 'ClientName': 'Test Client 2'}])
    ]

    result_queue = mp.Queue()
    processes = []

    try:
        # Start workers
        for campaign, credentials in test_campaigns:
            p = mp.Process(target=simple_worker, args=(campaign, credentials, result_queue))
            p.start()
            processes.append(p)
            logger.info(f"Started process for {campaign}")

        # Collect results
        results = []
        for _ in range(len(test_campaigns)):
            try:
                result = result_queue.get(timeout=10)
                results.append(result)
                logger.info(f"Got result: {result}")
            except Exception as e:
                logger.error(f"Error getting result: {e}")

        # Wait for processes
        for p in processes:
            p.join(timeout=5)
            if p.is_alive():
                logger.warning(f"Process {p.pid} didn't finish, terminating...")
                p.terminate()

        logger.info(f"Simple multiprocessing test completed. Results: {len(results)}")
        return len(results) == len(test_campaigns)

    except Exception as e:
        logger.error(f"Error in simple multiprocessing test: {e}")
        return False


async def run_tests():
    """Run all tests"""
    logger.info("=" * 60)
    logger.info("MULTIPROCESSING DEBUG TESTS")
    logger.info("=" * 60)

    # Test 1: Data fetching
    logger.info("\n--- Test 1: Data Fetching ---")
    data_test_passed = await test_data_fetching()
    logger.info(f"Data fetching test: {'PASSED' if data_test_passed else 'FAILED'}")

    # Test 2: Simple multiprocessing
    logger.info("\n--- Test 2: Simple Multiprocessing ---")
    mp_test_passed = test_simple_multiprocessing()
    logger.info(f"Simple multiprocessing test: {'PASSED' if mp_test_passed else 'FAILED'}")

    # Summary
    logger.info("\n--- Test Summary ---")
    logger.info(f"Data fetching: {'✓' if data_test_passed else '✗'}")
    logger.info(f"Multiprocessing: {'✓' if mp_test_passed else '✗'}")

    if data_test_passed and mp_test_passed:
        logger.info("All tests PASSED! Multiprocessing should work.")
        return True
    else:
        logger.error("Some tests FAILED. Check the errors above.")
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(run_tests())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Critical error in tests: {e}")
        sys.exit(1)