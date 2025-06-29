#!/usr/bin/env python3
"""
Setup script for SEC Financial Data Pipeline.
Initializes the system and fetches sample data.
"""

import asyncio
import click
import logging
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import setup_logging, get_config
from src.etl.pipeline import fetch_ticker_on_demand
from src.etl.data_manager import DataManager


@click.command()
@click.option('--sample-tickers', default="AAPL,MSFT,GOOGL", help='Comma-separated list of sample tickers to fetch')
@click.option('--skip-fetch', is_flag=True, help='Skip fetching sample data')
def setup(sample_tickers, skip_fetch):
    """Initialize the SEC Financial Data Pipeline and fetch sample data."""
    
    # Setup logging
    logger = setup_logging()
    logger.info("Setting up SEC Financial Data Pipeline...")
    
    # Load and validate configuration
    try:
        config = get_config()
        logger.info(f"Configuration loaded successfully")
        logger.info(f"Data storage path: {config.data_storage.company_facts_path}")
        logger.info(f"API will run on {config.api.host}:{config.api.port}")
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return False
    
    # Create necessary directories
    try:
        data_path = Path(config.data_storage.company_facts_path)
        data_path.mkdir(parents=True, exist_ok=True)
        
        logs_path = Path("logs")
        logs_path.mkdir(exist_ok=True)
        
        logger.info("Created necessary directories")
    except Exception as e:
        logger.error(f"Failed to create directories: {e}")
        return False
    
    # Initialize data manager
    try:
        data_manager = DataManager()
        stats = data_manager.calculate_storage_stats()
        logger.info(f"Data manager initialized. Current stats: {stats}")
    except Exception as e:
        logger.error(f"Failed to initialize data manager: {e}")
        return False
    
    # Fetch sample data
    if not skip_fetch:
        tickers = [t.strip().upper() for t in sample_tickers.split(',')]
        logger.info(f"Fetching sample data for tickers: {tickers}")
        
        async def fetch_samples():
            success_count = 0
            for ticker in tickers:
                try:
                    logger.info(f"Fetching data for {ticker}...")
                    job = await fetch_ticker_on_demand(ticker)
                    
                    if job.status.value == "completed":
                        logger.info(f"✓ {ticker}: {job.records_processed} records, {len(job.files_created)} files")
                        success_count += 1
                    else:
                        logger.error(f"✗ {ticker}: {job.error_message}")
                        
                except Exception as e:
                    logger.error(f"✗ {ticker}: {e}")
            
            return success_count
        
        try:
            success_count = asyncio.run(fetch_samples())
            logger.info(f"Sample data fetch completed: {success_count}/{len(tickers)} successful")
        except Exception as e:
            logger.error(f"Failed to fetch sample data: {e}")
            return False
    
    # Print setup summary
    click.echo("\n" + "="*60)
    click.echo("🚀 SEC Financial Data Pipeline Setup Complete!")
    click.echo("="*60)
    
    if not skip_fetch:
        # Show data summary
        try:
            stats = data_manager.calculate_storage_stats()
            click.echo(f"\n📊 Data Summary:")
            click.echo(f"   Available tickers: {stats['total_tickers']}")
            click.echo(f"   Total files: {stats['total_files']}")
            click.echo(f"   Storage used: {stats['total_size_mb']} MB")
            click.echo(f"   Total records: {stats['total_records']:,}")
        except Exception as e:
            click.echo(f"\n❌ Could not generate data summary: {e}")
    
    click.echo(f"\n🔧 Configuration:")
    click.echo(f"   Data path: {config.data_storage.company_facts_path}")
    click.echo(f"   API endpoint: http://{config.api.host}:{config.api.port}")
    click.echo(f"   S&P 500 tickers configured: {len(config.sp500_tickers)}")
    
    click.echo(f"\n📚 Next Steps:")
    click.echo(f"   1. Start the API server:")
    click.echo(f"      uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload")
    click.echo(f"")
    click.echo(f"   2. View API documentation:")
    click.echo(f"      http://localhost:8000/docs")
    click.echo(f"")
    click.echo(f"   3. Fetch more data:")
    click.echo(f"      python scripts/run_etl.py fetch NVDA")
    click.echo(f"      python scripts/run_etl.py incremental --tickers 'TSLA,META,NFLX'")
    click.echo(f"")
    click.echo(f"   4. Run daily ETL:")
    click.echo(f"      python scripts/run_etl.py daily")
    click.echo(f"")
    click.echo(f"   5. Check status:")
    click.echo(f"      python scripts/run_etl.py status")
    
    click.echo(f"\n💡 Example API calls:")
    click.echo(f"   curl 'http://localhost:8000/status'")
    click.echo(f"   curl 'http://localhost:8000/financials/AAPL'")
    click.echo(f"   curl 'http://localhost:8000/financials/AAPL/revenue'")
    
    click.echo("\n" + "="*60)
    return True


if __name__ == '__main__':
    setup() 