"""
Sampling Tool - Database Version
Run this script to start the sampling tool with database backend.
"""

import sys
import os
import logging
import argparse
import subprocess


def setup_logging(verbose=False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def check_database(db_path, logger):
    """Check if database exists and is initialized."""
    if not os.path.exists(db_path):
        logger.warning(f"Database not found at {db_path}")
        return False

    # Try to import and check database
    try:
        from database import Database
        db = Database.get_instance(db_path)
        # This will raise an error if tables are missing
        logger.info("Database connection successful")
        return True
    except Exception as e:
        logger.error(f"Database check failed: {e}")
        return False


def initialize_database(db_path, csv_path=None, logger=None):
    """Initialize the database if needed."""
    logger.info("Initializing database...")

    cmd = [sys.executable, "db_init.py", "--db-path", db_path]

    if csv_path:
        cmd.extend(["--csv-path", csv_path])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("Database initialized successfully")
            return True
        else:
            logger.error(f"Database initialization failed: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"Failed to run initialization script: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Run the Sampling Tool')
    parser.add_argument(
        '--db-path',
        default='./data/sampling.db',
        help='Path to the SQLite database file'
    )
    parser.add_argument(
        '--init-csv',
        help='CSV file to import during initialization'
    )
    parser.add_argument(
        '--force-init',
        action='store_true',
        help='Force database initialization even if it exists'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()
    logger = setup_logging(args.verbose)

    logger.info("Starting Sampling Tool")

    # Check database
    db_exists = check_database(args.db_path, logger)

    if not db_exists or args.force_init:
        # Initialize database
        if not initialize_database(args.db_path, args.init_csv, logger):
            logger.error("Failed to initialize database. Exiting.")
            return 1

    # Import and run the main application
    try:
        logger.info("Launching application...")
        from sample_testing_main import main as run_app
        run_app()

    except ImportError as e:
        logger.error(f"Failed to import application modules: {e}")
        logger.error("Make sure all required files are in the current directory:")
        logger.error("  - sample_testing_main.py")
        logger.error("  - ui_tkinter.py")
        logger.error("  - database.py")
        logger.error("  - singleton.py")
        return 1

    except Exception as e:
        logger.error(f"Application error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
