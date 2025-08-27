"""
Windows compatibility fixes for multiprocessing
"""
import os
import sys
import multiprocessing as mp


def setup_windows_multiprocessing():
    """Set up multiprocessing for Windows compatibility"""
    if os.name == 'nt':  # Windows
        # Force spawn method on Windows to avoid pickling issues
        try:
            mp.set_start_method('spawn', force=True)
        except RuntimeError:
            # Already set, ignore
            pass

        # Add current directory to Python path for worker processes
        if hasattr(sys, 'frozen'):
            # Running as executable
            current_dir = os.path.dirname(sys.executable)
        else:
            # Running as script
            current_dir = os.path.dirname(os.path.abspath(__file__))

        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)


def ensure_module_imports():
    """Ensure all required modules can be imported in worker processes"""
    try:
        # Pre-import modules that might cause issues
        import asyncio
        import aiohttp
        import pandas as pd
        import google.cloud.bigquery as bigquery
        from bs4 import BeautifulSoup
        return True
    except ImportError as e:
        print(f"Warning: Could not import required modules: {e}")
        return False


# Call setup when module is imported
if __name__ != "__main__":
    setup_windows_multiprocessing()