#!/usr/bin/env python3
"""
Command-line ETL runner for SEC Financial Data Pipeline.
Provides options for running incremental, full refresh, and on-demand ETL jobs.
"""

import asyncio
import click
import logging
import sys
from pathlib import Path
from typing import List, Optional

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import setup_logging, get_config, get_ticker_list
from src.etl.pipeline import ETLPipeline, run_daily_etl, run_full_data_refresh, fetch_ticker_on_demand
from src.etl.data_manager import DataManager


@click.group()
@click.option('--debug', is_flag=True, help='Enable debug logging')
def cli(debug):
    """SEC Financial Data ETL Pipeline Runner."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Setup logging
    logger = setup_logging()
    logger.info("SEC Financial Data ETL Pipeline")


@cli.command()
@click.option('--tickers', '-t', help='Comma-separated list of tickers to process')
@click.option('--max-concurrent', '-c', type=int, help='Maximum concurrent downloads')
def incremental(tickers, max_concurrent):
    """Run incremental ETL for configured tickers."""
    async def run():
        pipeline = ETLPipeline()
        
        if max_concurrent:
            pipeline.config.etl.max_concurrent_downloads = max_concurrent
        
        ticker_list = None
        if tickers:
            ticker_list = [t.strip().upper() for t in tickers.split(',')]
        
        jobs = await pipeline.run_incremental_etl(ticker_list)
        
        # Print summary
        completed = sum(1 for job in jobs if job.status.value == "completed")
        failed = sum(1 for job in jobs if job.status.value == "failed")
        
        click.echo(f"\nETL Summary:")
        click.echo(f"  Total jobs: {len(jobs)}")
        click.echo(f"  Completed: {completed}")
        click.echo(f"  Failed: {failed}")
        
        if failed > 0:
            click.echo("\nFailed jobs:")
            for job in jobs:
                if job.status.value == "failed":
                    click.echo(f"  {job.ticker}: {job.error_message}")
    
    asyncio.run(run())


@cli.command()
@click.option('--tickers', '-t', help='Comma-separated list of tickers to refresh')
@click.confirmation_option(prompt='This will delete and re-fetch all data. Continue?')
def full_refresh(tickers):
    """Run full data refresh (deletes and re-fetches all data)."""
    async def run():
        pipeline = ETLPipeline()
        
        ticker_list = None
        if tickers:
            ticker_list = [t.strip().upper() for t in tickers.split(',')]
        
        jobs = await pipeline.run_full_refresh(ticker_list)
        
        # Print summary
        completed = sum(1 for job in jobs if job.status.value == "completed")
        failed = sum(1 for job in jobs if job.status.value == "failed")
        
        click.echo(f"\nFull Refresh Summary:")
        click.echo(f"  Total jobs: {len(jobs)}")
        click.echo(f"  Completed: {completed}")
        click.echo(f"  Failed: {failed}")
    
    asyncio.run(run())


@cli.command()
@click.argument('ticker')
def fetch(ticker):
    """Fetch data for a specific ticker on-demand."""
    async def run():
        job = await fetch_ticker_on_demand(ticker.upper())
        
        click.echo(f"\nFetch job for {ticker.upper()}:")
        click.echo(f"  Status: {job.status}")
        click.echo(f"  Records processed: {job.records_processed}")
        click.echo(f"  Files created: {len(job.files_created)}")
        
        if job.error_message:
            click.echo(f"  Error: {job.error_message}")
    
    asyncio.run(run())


@cli.command()
def daily():
    """Run the daily ETL process."""
    async def run():
        jobs = await run_daily_etl()
        
        click.echo(f"Daily ETL completed: {len(jobs)} jobs processed")
        
        completed = sum(1 for job in jobs if job.status.value == "completed")
        failed = sum(1 for job in jobs if job.status.value == "failed")
        
        click.echo(f"  Completed: {completed}")
        click.echo(f"  Failed: {failed}")
    
    asyncio.run(run())


@cli.command()
def status():
    """Show ETL pipeline status and statistics."""
    async def run():
        pipeline = ETLPipeline()
        data_manager = DataManager()
        
        # Pipeline stats
        pipeline_stats = pipeline.get_pipeline_stats()
        storage_stats = data_manager.calculate_storage_stats()
        
        click.echo("ETL Pipeline Status:")
        click.echo(f"  Total jobs run: {pipeline_stats['total_jobs']}")
        click.echo(f"  Success rate: {pipeline_stats['success_rate']:.1f}%")
        click.echo(f"  Last run: {pipeline_stats['last_run']}")
        click.echo(f"  Active jobs: {pipeline_stats['active_jobs']}")
        
        click.echo("\nData Storage:")
        click.echo(f"  Total tickers: {storage_stats['total_tickers']}")
        click.echo(f"  Total files: {storage_stats['total_files']}")
        click.echo(f"  Total size: {storage_stats['total_size_mb']} MB")
        click.echo(f"  Total records: {storage_stats['total_records']:,}")
        
        # Recent jobs
        recent_jobs = pipeline.get_job_history(10)
        if recent_jobs:
            click.echo("\nRecent Jobs:")
            for job in recent_jobs[-5:]:  # Show last 5
                status_color = 'green' if job.status.value == 'completed' else 'red'
                click.echo(f"  {job.ticker}: ", nl=False)
                click.secho(job.status.value, fg=status_color, nl=False)
                if job.completed_at:
                    click.echo(f" ({job.completed_at.strftime('%Y-%m-%d %H:%M')})")
                else:
                    click.echo()
    
    asyncio.run(run())


@cli.command()
def list_tickers():
    """List available tickers and configured S&P 500 companies."""
    data_manager = DataManager()
    
    # Available tickers with data
    available = data_manager.list_available_tickers()
    click.echo(f"Available tickers with data ({len(available)}):")
    for ticker in sorted(available):
        freshness = data_manager.get_data_freshness(ticker)
        if freshness:
            years = len(freshness.annual_data_years)
            click.echo(f"  {ticker}: {years} years of data")
    
    # Configured tickers
    configured = get_ticker_list()
    click.echo(f"\nConfigured S&P 500 tickers ({len(configured)}):")
    click.echo("  " + ", ".join(configured[:20]) + "..." if len(configured) > 20 else "  " + ", ".join(configured))


@cli.command()
@click.argument('ticker')
def info(ticker):
    """Show detailed information about a specific ticker."""
    data_manager = DataManager()
    ticker = ticker.upper()
    
    freshness = data_manager.get_data_freshness(ticker)
    if not freshness:
        click.echo(f"No data found for {ticker}")
        return
    
    file_info = data_manager.get_ticker_file_info(ticker)
    
    click.echo(f"Ticker: {ticker}")
    click.echo(f"Last updated: {freshness.last_updated}")
    click.echo(f"Last SEC filing: {freshness.last_sec_filing_date}")
    click.echo(f"Annual years: {freshness.annual_data_years}")
    click.echo(f"Quarterly periods: {len(freshness.quarterly_data_periods)}")
    
    click.echo(f"\nFiles ({len(file_info)}):")
    for file in file_info:
        size_mb = round(file.file_size_bytes / (1024*1024), 2) if file.file_size_bytes else 0
        click.echo(f"  {file.year} Q{file.quarter if file.quarter else 'Annual'}: {file.record_count} records, {size_mb} MB")


@cli.command()
@click.argument('ticker')
@click.confirmation_option(prompt='This will delete all data for the ticker. Continue?')
def delete(ticker):
    """Delete all data for a specific ticker."""
    data_manager = DataManager()
    ticker = ticker.upper()
    
    success = data_manager.delete_ticker_data(ticker)
    if success:
        click.echo(f"Successfully deleted data for {ticker}")
    else:
        click.echo(f"Failed to delete data for {ticker}")


if __name__ == '__main__':
    cli() 