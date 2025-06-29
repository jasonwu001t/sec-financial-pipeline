"""
FastAPI application for SEC Financial Data Pipeline.
Provides REST API endpoints for accessing financial data.
"""

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Depends
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import asyncio
import io
import json
import pandas as pd
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

from core.config import get_config, setup_logging
from core.models import (
    FinancialDataResponse, MetricResponse, HealthCheckResponse, ErrorResponse,
    FinancialQuery, MetricQuery, ComparisonQuery, ReportingPeriod
)
from etl.pipeline import ETLPipeline, fetch_ticker_on_demand
from etl.data_manager import DataManager
from .cache import CacheManager
from .data_service import DataService


# Setup logging
logger = setup_logging()

# Load configuration
config = get_config()

# Initialize FastAPI app
app = FastAPI(
    title=config.api.title,
    description=config.api.description,
    version=config.api.version,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.performance.cors_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if config.performance.enable_compression:
    app.add_middleware(GZipMiddleware, minimum_size=1000)

# Initialize services
data_manager = DataManager()
cache_manager = CacheManager()
data_service = DataService(data_manager, cache_manager)
etl_pipeline = ETLPipeline()


# Dependency to get data service
def get_data_service() -> DataService:
    return data_service


# Health check endpoint
@app.get("/status", response_model=HealthCheckResponse)
async def health_check():
    """Health check and system status endpoint."""
    try:
        # Get data freshness stats
        available_tickers = data_manager.list_available_tickers()
        storage_stats = data_manager.calculate_storage_stats()
        pipeline_stats = etl_pipeline.get_pipeline_stats()
        cache_stats = cache_manager.get_stats()
        
        # Determine overall health
        status = "healthy"
        if not available_tickers:
            status = "no_data"
        elif pipeline_stats.get("success_rate", 0) < 80:
            status = "degraded"
        
        return HealthCheckResponse(
            status=status,
            api_version=config.api.version,
            data_freshness={
                "available_tickers": len(available_tickers),
                "total_files": storage_stats.get("total_files", 0),
                "total_size_mb": storage_stats.get("total_size_mb", 0),
                "last_etl_run": pipeline_stats.get("last_run"),
                "etl_success_rate": pipeline_stats.get("success_rate", 0)
            },
            cache_stats=cache_stats
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail="Health check failed")


# Financial data endpoints
@app.get("/financials/{ticker}", response_model=FinancialDataResponse)
async def get_financial_data(
    ticker: str,
    period: ReportingPeriod = Query(ReportingPeriod.ANNUAL, description="Reporting period"),
    years: int = Query(5, ge=1, le=20, description="Number of years of data"),
    format: str = Query("json", description="Response format (json, csv, parquet)"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    service: DataService = Depends(get_data_service)
):
    """Get financial data for a specific ticker."""
    try:
        ticker = ticker.upper()
        
        # Check if ticker data exists
        company_data = await service.get_company_data(ticker, years)
        
        if company_data is None:
            # Trigger on-demand fetch
            logger.info(f"No data found for {ticker}, triggering on-demand fetch")
            background_tasks.add_task(fetch_ticker_on_demand, ticker)
            
            raise HTTPException(
                status_code=202,
                detail=f"Data for {ticker} not available. Fetching in background. Please try again in a few minutes."
            )
        
        # Handle different response formats
        if format.lower() == "csv":
            csv_data = service.convert_to_csv(company_data, period)
            return StreamingResponse(
                io.StringIO(csv_data),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={ticker}_financials.csv"}
            )
        elif format.lower() == "parquet":
            parquet_data = service.convert_to_parquet(company_data, period)
            return StreamingResponse(
                io.BytesIO(parquet_data),
                media_type="application/octet-stream",
                headers={"Content-Disposition": f"attachment; filename={ticker}_financials.parquet"}
            )
        
        # Default JSON response
        return FinancialDataResponse(
            data=company_data,
            ticker=ticker,
            period=period,
            years=years,
            message=f"Financial data for {ticker}"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching financial data for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/financials/{ticker}/{metric}", response_model=MetricResponse)
async def get_financial_metric(
    ticker: str,
    metric: str,
    period: ReportingPeriod = Query(ReportingPeriod.ANNUAL, description="Reporting period"),
    years: int = Query(5, ge=1, le=20, description="Number of years of data"),
    format: str = Query("json", description="Response format (json, csv)"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    service: DataService = Depends(get_data_service)
):
    """Get a specific financial metric for a ticker."""
    try:
        ticker = ticker.upper()
        
        # Get metric data
        metric_data = await service.get_metric_data(ticker, metric, period, years)
        
        if metric_data is None:
            # Trigger on-demand fetch
            background_tasks.add_task(fetch_ticker_on_demand, ticker)
            raise HTTPException(
                status_code=202,
                detail=f"Data for {ticker} not available. Fetching in background."
            )
        
        # Handle CSV format
        if format.lower() == "csv":
            csv_data = service.convert_metric_to_csv(metric_data, ticker, metric)
            return StreamingResponse(
                io.StringIO(csv_data),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={ticker}_{metric}.csv"}
            )
        
        return MetricResponse(
            data=metric_data,
            ticker=ticker,
            metric=metric,
            period=period,
            years=years,
            message=f"{metric} data for {ticker}"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching {metric} for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/financials/compare", response_model=FinancialDataResponse)
async def compare_companies(
    query: ComparisonQuery,
    service: DataService = Depends(get_data_service)
):
    """Compare financial metrics across multiple companies."""
    try:
        comparison_data = await service.compare_companies(
            query.tickers, query.metric, query.period, query.years
        )
        
        return FinancialDataResponse(
            data=comparison_data,
            message=f"Comparison of {query.metric} across {len(query.tickers)} companies"
        )
        
    except Exception as e:
        logger.error(f"Error comparing companies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Data management endpoints
@app.get("/data/tickers")
async def list_available_tickers(
    service: DataService = Depends(get_data_service)
):
    """List all available tickers with data."""
    try:
        tickers = data_manager.list_available_tickers()
        freshness_info = {}
        
        for ticker in tickers[:50]:  # Limit to first 50 for performance
            freshness = data_manager.get_data_freshness(ticker)
            if freshness:
                freshness_info[ticker] = {
                    "last_updated": freshness.last_updated,
                    "annual_years": freshness.annual_data_years,
                    "quarterly_periods": len(freshness.quarterly_data_periods)
                }
        
        return {
            "total_tickers": len(tickers),
            "tickers": tickers,
            "freshness_sample": freshness_info
        }
        
    except Exception as e:
        logger.error(f"Error listing tickers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/ticker/{ticker}/info")
async def get_ticker_info(ticker: str):
    """Get detailed information about a specific ticker's data."""
    try:
        ticker = ticker.upper()
        
        freshness = data_manager.get_data_freshness(ticker)
        if not freshness:
            raise HTTPException(status_code=404, detail=f"No data found for {ticker}")
        
        file_info = data_manager.get_ticker_file_info(ticker)
        
        return {
            "ticker": ticker,
            "data_freshness": freshness.dict(),
            "files": [
                {
                    "file_path": f.file_path,
                    "year": f.year,
                    "quarter": f.quarter,
                    "statement_type": f.statement_type,
                    "record_count": f.record_count,
                    "file_size_mb": round(f.file_size_bytes / (1024*1024), 2) if f.file_size_bytes else None
                }
                for f in file_info
            ],
            "total_files": len(file_info),
            "total_records": sum(f.record_count or 0 for f in file_info)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting ticker info for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ETL management endpoints
@app.post("/etl/fetch/{ticker}")
async def trigger_ticker_fetch(
    ticker: str,
    background_tasks: BackgroundTasks
):
    """Trigger on-demand fetch for a specific ticker."""
    try:
        ticker = ticker.upper()
        
        # Add background task
        background_tasks.add_task(fetch_ticker_on_demand, ticker)
        
        return {
            "message": f"Fetch triggered for {ticker}",
            "ticker": ticker,
            "status": "in_progress"
        }
        
    except Exception as e:
        logger.error(f"Error triggering fetch for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/etl/refresh")
async def trigger_full_refresh(background_tasks: BackgroundTasks):
    """Trigger full data refresh for all tickers."""
    try:
        from etl.pipeline import run_full_data_refresh
        
        background_tasks.add_task(run_full_data_refresh)
        
        return {
            "message": "Full data refresh triggered",
            "status": "in_progress"
        }
        
    except Exception as e:
        logger.error(f"Error triggering full refresh: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/etl/status")
async def get_etl_status():
    """Get ETL pipeline status and statistics."""
    try:
        pipeline_stats = etl_pipeline.get_pipeline_stats()
        recent_jobs = etl_pipeline.get_job_history(20)
        
        return {
            "pipeline_stats": pipeline_stats,
            "recent_jobs": [
                {
                    "job_id": job.job_id,
                    "ticker": job.ticker,
                    "job_type": job.job_type,
                    "status": job.status,
                    "started_at": job.started_at,
                    "completed_at": job.completed_at,
                    "records_processed": job.records_processed,
                    "error_message": job.error_message
                }
                for job in recent_jobs
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting ETL status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Cache management endpoints
@app.get("/cache/stats")
async def get_cache_stats():
    """Get cache statistics."""
    try:
        return cache_manager.get_stats()
    except Exception as e:
        logger.error(f"Error getting cache stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/cache/clear")
async def clear_cache():
    """Clear all cache entries."""
    try:
        cleared_count = await cache_manager.clear_all()
        return {
            "message": f"Cleared {cleared_count} cache entries",
            "cleared_count": cleared_count
        }
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Storage statistics endpoint
@app.get("/storage/stats")
async def get_storage_stats():
    """Get storage statistics."""
    try:
        stats = data_manager.calculate_storage_stats()
        return stats
    except Exception as e:
        logger.error(f"Error getting storage stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Error handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return ErrorResponse(
        success=False,
        error_type="not_found",
        message="Resource not found",
        details={"path": str(request.url)}
    )


@app.exception_handler(500)
async def internal_error_handler(request, exc):
    logger.error(f"Internal server error: {exc}")
    return ErrorResponse(
        success=False,
        error_type="internal_error",
        message="Internal server error",
        details={"error": str(exc)}
    )


# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    logger.info("Starting SEC Financial Data API")
    
    # Initialize cache
    await cache_manager.initialize()
    
    # Validate configuration
    from core.config import config_manager
    config_manager.validate_config()
    
    logger.info(f"API server started on {config.api.host}:{config.api.port}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("Shutting down SEC Financial Data API")
    
    # Close cache connections
    await cache_manager.close()
    
    logger.info("API server shutdown complete")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "src.api.main:app",
        host=config.api.host,
        port=config.api.port,
        reload=config.api.reload,
        workers=1,  # Single worker for development
        log_level="info"
    ) 