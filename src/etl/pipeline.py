"""
ETL Pipeline for SEC Financial Data.
Handles incremental data extraction, transformation, and loading.
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from pathlib import Path
import json

from ..core.config import get_config, get_ticker_list
from ..core.sec_client import SECAPIClient
from ..core.models import ETLJob, ETLJobStatus, CompanyInfo, FinancialFact
from .data_manager import DataManager


class ETLPipeline:
    """Main ETL pipeline for SEC financial data."""
    
    def __init__(self):
        self.config = get_config()
        self.logger = logging.getLogger(__name__)
        self.data_manager = DataManager()
        
        # Job tracking
        self._active_jobs: Dict[str, ETLJob] = {}
        self._job_history: List[ETLJob] = []
        
        # Load job history
        self._load_job_history()
    
    def _load_job_history(self):
        """Load job history from disk."""
        try:
            history_file = Path(self.data_manager.metadata_path) / "job_history.json"
            if history_file.exists():
                with open(history_file, 'r') as f:
                    job_data = json.load(f)
                    self._job_history = [ETLJob(**job) for job in job_data]
                    
        except Exception as e:
            self.logger.warning(f"Failed to load job history: {e}")
    
    def _save_job_history(self):
        """Save job history to disk."""
        try:
            history_file = Path(self.data_manager.metadata_path) / "job_history.json"
            job_data = [job.dict() for job in self._job_history[-100:]]  # Keep last 100 jobs
            
            with open(history_file, 'w') as f:
                json.dump(job_data, f, indent=2, default=str)
                
        except Exception as e:
            self.logger.error(f"Failed to save job history: {e}")
    
    def create_job(self, ticker: str, job_type: str = "incremental") -> ETLJob:
        """Create a new ETL job."""
        job = ETLJob(
            job_id=str(uuid.uuid4()),
            ticker=ticker.upper(),
            job_type=job_type,
            status=ETLJobStatus.PENDING
        )
        
        self._active_jobs[job.job_id] = job
        return job
    
    def get_job_status(self, job_id: str) -> Optional[ETLJob]:
        """Get the status of a job."""
        return self._active_jobs.get(job_id)
    
    async def run_incremental_etl(self, tickers: Optional[List[str]] = None) -> List[ETLJob]:
        """Run incremental ETL for specified tickers or all configured tickers."""
        if tickers is None:
            tickers = get_ticker_list()
        
        self.logger.info(f"Starting incremental ETL for {len(tickers)} tickers")
        
        # Determine which tickers need updates
        tickers_to_update = []
        for ticker in tickers:
            freshness = self.data_manager.get_data_freshness(ticker)
            
            if freshness is None:
                # No data exists, needs full load
                tickers_to_update.append(ticker)
                self.logger.info(f"{ticker}: No existing data, scheduling full load")
            elif freshness.needs_update:
                # Explicitly marked for update
                tickers_to_update.append(ticker)
                self.logger.info(f"{ticker}: Marked for update")
            elif self._should_update(freshness):
                # Check if update is needed based on age
                tickers_to_update.append(ticker)
                self.logger.info(f"{ticker}: Data is stale, scheduling update")
            else:
                self.logger.debug(f"{ticker}: Data is fresh, skipping")
        
        if not tickers_to_update:
            self.logger.info("No tickers need updating")
            return []
        
        self.logger.info(f"Processing {len(tickers_to_update)} tickers for updates")
        
        # Create jobs for all tickers
        jobs = []
        for ticker in tickers_to_update:
            job = self.create_job(ticker, "incremental")
            jobs.append(job)
        
        # Process jobs with controlled concurrency
        max_concurrent = self.config.etl.max_concurrent_downloads
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_job(job: ETLJob):
            async with semaphore:
                await self._execute_job(job)
        
        # Execute all jobs
        tasks = [process_job(job) for job in jobs]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Update job history
        self._job_history.extend(jobs)
        self._save_job_history()
        
        # Clean up active jobs
        for job in jobs:
            if job.job_id in self._active_jobs:
                del self._active_jobs[job.job_id]
        
        # Log summary
        completed = sum(1 for job in jobs if job.status == ETLJobStatus.COMPLETED)
        failed = sum(1 for job in jobs if job.status == ETLJobStatus.FAILED)
        
        self.logger.info(f"ETL completed: {completed} successful, {failed} failed")
        
        return jobs
    
    async def run_on_demand_etl(self, ticker: str) -> ETLJob:
        """Run on-demand ETL for a specific ticker."""
        self.logger.info(f"Starting on-demand ETL for {ticker}")
        
        job = self.create_job(ticker, "on-demand")
        await self._execute_job(job)
        
        # Add to history and clean up
        self._job_history.append(job)
        self._save_job_history()
        
        if job.job_id in self._active_jobs:
            del self._active_jobs[job.job_id]
        
        return job
    
    async def _execute_job(self, job: ETLJob):
        """Execute a single ETL job."""
        ticker = job.ticker
        job.status = ETLJobStatus.RUNNING
        job.started_at = datetime.utcnow()
        
        try:
            self.logger.info(f"Processing {ticker} (Job: {job.job_id})")
            
            # Check if we need to update this ticker
            if job.job_type == "incremental" and not self._needs_update(ticker):
                job.status = ETLJobStatus.COMPLETED
                job.completed_at = datetime.utcnow()
                job.metadata["skipped"] = True
                job.metadata["reason"] = "Data is fresh"
                return
            
            # Fetch data from SEC
            async with SECAPIClient() as sec_client:
                try:
                    company_info, facts = await sec_client.fetch_company_data(ticker)
                    
                    if not facts:
                        raise ValueError(f"No facts returned for {ticker}")
                    
                    # Check if data has changed (for incremental updates)
                    if job.job_type == "incremental" and self.config.etl.skip_unchanged:
                        if not self._has_data_changed(ticker, facts):
                            job.status = ETLJobStatus.COMPLETED
                            job.completed_at = datetime.utcnow()
                            job.metadata["skipped"] = True
                            job.metadata["reason"] = "Data unchanged"
                            return
                    
                    # Save data to parquet files
                    parquet_files = self.data_manager.save_company_data(company_info, facts)
                    
                    # Update job metadata
                    job.records_processed = len(facts)
                    job.files_created = [str(pf.file_path) for pf in parquet_files]
                    job.metadata.update({
                        "company_name": company_info.name,
                        "cik": company_info.cik,
                        "files_count": len(parquet_files),
                        "latest_filing_year": max((f.fiscal_year for f in facts if f.fiscal_year), default=None)
                    })
                    
                    job.status = ETLJobStatus.COMPLETED
                    job.completed_at = datetime.utcnow()
                    
                    self.logger.info(f"Completed {ticker}: {len(facts)} facts, {len(parquet_files)} files")
                    
                except Exception as e:
                    raise Exception(f"Failed to fetch/process data for {ticker}: {e}")
                    
        except Exception as e:
            job.status = ETLJobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            job.metadata["error_details"] = str(e)
            
            self.logger.error(f"Job failed for {ticker}: {e}")
    
    def _should_update(self, freshness) -> bool:
        """Check if a ticker should be updated based on freshness."""
        if freshness.needs_update:
            return True
        
        # Check age of data
        age_hours = (datetime.utcnow() - freshness.last_updated).total_seconds() / 3600
        
        # Update daily for recent data, weekly for older data
        if freshness.last_sec_filing_date:
            days_since_filing = (datetime.utcnow().date() - freshness.last_sec_filing_date).days
            
            if days_since_filing < 30:  # Recent filings
                return age_hours > 24  # Update daily
            elif days_since_filing < 90:  # Quarterly filings
                return age_hours > 24 * 3  # Update every 3 days
            else:  # Older data
                return age_hours > 24 * 7  # Update weekly
        
        # Default to daily updates
        return age_hours > 24
    
    def _needs_update(self, ticker: str) -> bool:
        """Check if a ticker needs updating."""
        freshness = self.data_manager.get_data_freshness(ticker)
        
        if freshness is None:
            return True  # No data exists
        
        return self._should_update(freshness)
    
    def _has_data_changed(self, ticker: str, new_facts: List[FinancialFact]) -> bool:
        """Check if the data has changed compared to stored data."""
        # This is a simplified change detection
        # In a production system, you might want more sophisticated comparison
        
        existing_data = self.data_manager.load_company_data(ticker)
        if not existing_data:
            return True  # No existing data
        
        # Compare number of facts as a simple heuristic
        if len(new_facts) != len(existing_data.raw_facts):
            return True
        
        # Compare latest filing dates
        new_latest = max((f.end_date or f.instant_date for f in new_facts if f.end_date or f.instant_date), default=None)
        existing_latest = max(
            (f.end_date or f.instant_date for f in existing_data.raw_facts if f.end_date or f.instant_date), 
            default=None
        )
        
        if new_latest != existing_latest:
            return True
        
        return False  # Assume no change
    
    async def run_full_refresh(self, tickers: Optional[List[str]] = None) -> List[ETLJob]:
        """Run full refresh for specified tickers."""
        if tickers is None:
            tickers = get_ticker_list()
        
        self.logger.info(f"Starting full refresh for {len(tickers)} tickers")
        
        # Create jobs for all tickers
        jobs = []
        for ticker in tickers:
            job = self.create_job(ticker, "full-refresh")
            jobs.append(job)
        
        # Process jobs with controlled concurrency
        max_concurrent = self.config.etl.max_concurrent_downloads
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_job(job: ETLJob):
            async with semaphore:
                # Delete existing data first
                self.data_manager.delete_ticker_data(job.ticker)
                await self._execute_job(job)
        
        # Execute all jobs
        tasks = [process_job(job) for job in jobs]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Update job history
        self._job_history.extend(jobs)
        self._save_job_history()
        
        # Clean up active jobs
        for job in jobs:
            if job.job_id in self._active_jobs:
                del self._active_jobs[job.job_id]
        
        return jobs
    
    def get_job_history(self, limit: int = 50) -> List[ETLJob]:
        """Get recent job history."""
        return self._job_history[-limit:]
    
    def get_pipeline_stats(self) -> Dict[str, any]:
        """Get pipeline statistics."""
        recent_jobs = self._job_history[-100:]  # Last 100 jobs
        
        if not recent_jobs:
            return {
                "total_jobs": 0,
                "success_rate": 0.0,
                "avg_processing_time": 0.0,
                "last_run": None
            }
        
        completed_jobs = [j for j in recent_jobs if j.status == ETLJobStatus.COMPLETED]
        failed_jobs = [j for j in recent_jobs if j.status == ETLJobStatus.FAILED]
        
        # Calculate processing times for completed jobs
        processing_times = []
        for job in completed_jobs:
            if job.started_at and job.completed_at:
                duration = (job.completed_at - job.started_at).total_seconds()
                processing_times.append(duration)
        
        return {
            "total_jobs": len(recent_jobs),
            "completed_jobs": len(completed_jobs),
            "failed_jobs": len(failed_jobs),
            "success_rate": len(completed_jobs) / len(recent_jobs) * 100 if recent_jobs else 0,
            "avg_processing_time": sum(processing_times) / len(processing_times) if processing_times else 0,
            "last_run": max((j.completed_at for j in recent_jobs if j.completed_at), default=None),
            "active_jobs": len(self._active_jobs)
        }


# Convenience functions for external use

async def run_daily_etl():
    """Run the daily incremental ETL process."""
    pipeline = ETLPipeline()
    return await pipeline.run_incremental_etl()


async def fetch_ticker_on_demand(ticker: str) -> ETLJob:
    """Fetch a specific ticker on demand."""
    pipeline = ETLPipeline()
    return await pipeline.run_on_demand_etl(ticker)


async def run_full_data_refresh():
    """Run a full data refresh for all tickers."""
    pipeline = ETLPipeline()
    return await pipeline.run_full_refresh()


if __name__ == "__main__":
    # Test the pipeline
    async def test():
        pipeline = ETLPipeline()
        
        # Test on-demand fetch
        job = await pipeline.run_on_demand_etl("AAPL")
        print(f"Job status: {job.status}")
        
        # Test incremental ETL for a few tickers
        jobs = await pipeline.run_incremental_etl(["MSFT", "GOOGL"])
        for job in jobs:
            print(f"{job.ticker}: {job.status}")
        
        # Show stats
        stats = pipeline.get_pipeline_stats()
        print(f"Pipeline stats: {stats}")
    
    asyncio.run(test()) 