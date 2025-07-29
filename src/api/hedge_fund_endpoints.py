"""
Hedge Fund 13F API Endpoints
Provides REST API endpoints for hedge fund portfolio analysis.
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from fastapi import APIRouter, HTTPException, Query, Path
from fastapi.responses import JSONResponse

import sys
from pathlib import Path as PathLib
sys.path.append(str(PathLib(__file__).parent.parent))

from core.models import (
    Form13F, HedgeFundManager, Form13FResponse, PortfolioAnalysisResponse,
    HedgeFundManagerResponse, APIResponse
)
from core.sec_data_service import SECDataService
from sec_13f.managers.berkshire_hathaway import BerkshireHathawayHandler

# Create router
router = APIRouter(prefix="/hedge-funds", tags=["hedge-funds"])
logger = logging.getLogger(__name__)


@router.get("/managers", response_model=HedgeFundManagerResponse)
async def get_hedge_fund_managers():
    """Get list of famous hedge fund managers."""
    try:
        async with SECDataService() as service:
            managers = await service.get_hedge_fund_managers()
            
            return HedgeFundManagerResponse(
                data=managers,
                manager_count=len(managers),
                message=f"Successfully retrieved {len(managers)} hedge fund managers"
            )
            
    except Exception as e:
        logger.error(f"Error fetching hedge fund managers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/managers/{cik}", response_model=HedgeFundManagerResponse) 
async def get_hedge_fund_manager(
    cik: str = Path(..., description="CIK of the hedge fund manager")
):
    """Get information about a specific hedge fund manager."""
    try:
        async with SECDataService() as service:
            manager = await service.get_manager_by_cik(cik)
            
            if not manager:
                raise HTTPException(status_code=404, detail=f"Manager with CIK {cik} not found")
            
            return HedgeFundManagerResponse(
                data=manager,
                message=f"Successfully retrieved manager information for CIK {cik}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching manager {cik}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/filings/{cik}", response_model=Form13FResponse)
async def get_13f_filings(
    cik: str = Path(..., description="CIK of the hedge fund manager"),
    periods: int = Query(4, ge=1, le=20, description="Number of quarters to retrieve")
):
    """Get 13F filings for a hedge fund manager."""
    try:
        async with SECDataService() as service:
            filings = await service.get_13f_filings(cik, periods)
            
            if not filings:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No 13F filings found for CIK {cik}"
                )
            
            return Form13FResponse(
                data=filings,
                manager_name=filings[0].filing_manager_name if filings else None,
                period=f"{periods} quarters",
                message=f"Successfully retrieved {len(filings)} 13F filings"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching 13F filings for {cik}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/filings/{cik}/latest", response_model=Form13FResponse)
async def get_latest_13f_filing(
    cik: str = Path(..., description="CIK of the hedge fund manager")
):
    """Get the latest 13F filing for a hedge fund manager."""
    try:
        async with SECDataService() as service:
            filing = await service.get_latest_13f_filing(cik)
            
            if not filing:
                raise HTTPException(
                    status_code=404,
                    detail=f"No recent 13F filings found for CIK {cik}"
                )
            
            return Form13FResponse(
                data=filing,
                manager_name=filing.filing_manager_name,
                period="latest",
                message="Successfully retrieved latest 13F filing"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching latest 13F filing for {cik}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analysis/berkshire", response_model=Dict[str, Any])
async def get_berkshire_analysis(
    quarters: int = Query(4, ge=2, le=8, description="Number of quarters to analyze")
):
    """Get comprehensive analysis of Berkshire Hathaway's investment strategy."""
    try:
        async with BerkshireHathawayHandler() as handler:
            insights = await handler.get_berkshire_insights()
            
            if "error" in insights:
                raise HTTPException(status_code=404, detail=insights["error"])
            
            return {
                "success": True,
                "data": insights,
                "timestamp": datetime.utcnow().isoformat(),
                "message": "Successfully generated Berkshire Hathaway analysis"
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating Berkshire analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analysis/portfolio-changes/{cik}")
async def analyze_portfolio_changes(
    cik: str = Path(..., description="CIK of the hedge fund manager"),
    quarters: int = Query(4, ge=2, le=8, description="Number of quarters to analyze")
):
    """Analyze portfolio changes for a hedge fund manager."""
    try:
        async with SECDataService() as service:
            # Get historical filings
            filings = await service.get_13f_filings(cik, periods=quarters)
            
            if len(filings) < 2:
                raise HTTPException(
                    status_code=400,
                    detail="Need at least 2 quarters of data for portfolio change analysis"
                )
            
            # Analyze changes between latest and previous filing
            latest_filing = filings[0]
            previous_filing = filings[1]
            
            # Create holdings lookup
            latest_holdings = {h.cusip: h for h in latest_filing.holdings}
            previous_holdings = {h.cusip: h for h in previous_filing.holdings}
            
            # Find changes
            new_positions = [
                h for cusip, h in latest_holdings.items() 
                if cusip not in previous_holdings
            ]
            
            exited_positions = [
                h for cusip, h in previous_holdings.items()
                if cusip not in latest_holdings
            ]
            
            # Calculate portfolio change metrics
            portfolio_change_pct = (
                (latest_filing.total_value - previous_filing.total_value) / 
                previous_filing.total_value * 100
            ) if previous_filing.total_value > 0 else 0
            
            analysis = {
                "manager_info": {
                    "cik": cik,
                    "name": latest_filing.filing_manager_name
                },
                "analysis_period": {
                    "latest_period": latest_filing.period_end_date.isoformat(),
                    "previous_period": previous_filing.period_end_date.isoformat(),
                    "quarters_analyzed": len(filings)
                },
                "portfolio_summary": {
                    "latest_value": latest_filing.total_value,
                    "previous_value": previous_filing.total_value,
                    "change_percent": portfolio_change_pct,
                    "latest_holdings_count": latest_filing.total_holdings_count,
                    "previous_holdings_count": previous_filing.total_holdings_count
                },
                "position_changes": {
                    "new_positions_count": len(new_positions),
                    "exited_positions_count": len(exited_positions),
                    "new_positions": sorted(new_positions, key=lambda x: x.value, reverse=True)[:10],
                    "exited_positions": sorted(exited_positions, key=lambda x: x.value, reverse=True)[:10]
                }
            }
            
            return {
                "success": True,
                "data": analysis,
                "timestamp": datetime.utcnow().isoformat(),
                "message": f"Successfully analyzed portfolio changes for {quarters} quarters"
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing portfolio changes for {cik}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-holdings/{cik}")
async def get_top_holdings(
    cik: str = Path(..., description="CIK of the hedge fund manager"),
    limit: int = Query(10, ge=1, le=50, description="Number of top holdings to return")
):
    """Get top holdings for a hedge fund manager."""
    try:
        async with SECDataService() as service:
            filing = await service.get_latest_13f_filing(cik)
            
            if not filing:
                raise HTTPException(
                    status_code=404,
                    detail=f"No recent 13F filings found for CIK {cik}"
                )
            
            # Sort holdings by value and take top N
            top_holdings = sorted(filing.holdings, key=lambda x: x.value, reverse=True)[:limit]
            
            # Calculate percentages
            total_value = filing.total_value
            for holding in top_holdings:
                holding.percentage_of_portfolio = (holding.value / total_value * 100) if total_value > 0 else 0
            
            return {
                "success": True,
                "data": {
                    "manager_info": {
                        "cik": cik,
                        "name": filing.filing_manager_name
                    },
                    "filing_info": {
                        "filing_date": filing.filing_date.isoformat(),
                        "period_end_date": filing.period_end_date.isoformat(),
                        "total_value": filing.total_value,
                        "total_holdings": filing.total_holdings_count
                    },
                    "top_holdings": top_holdings
                },
                "timestamp": datetime.utcnow().isoformat(),
                "message": f"Successfully retrieved top {len(top_holdings)} holdings"
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching top holdings for {cik}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def hedge_fund_api_health():
    """Health check endpoint for hedge fund API."""
    try:
        async with SECDataService() as service:
            health = await service.get_service_health()
            
            # Check specifically for 13F functionality
            hedge_fund_status = "healthy"
            managers = await service.get_hedge_fund_managers()
            
            if not managers:
                hedge_fund_status = "degraded"
            
            return {
                "status": hedge_fund_status,
                "service_health": health,
                "managers_available": len(managers),
                "timestamp": datetime.utcnow().isoformat(),
                "message": "Hedge fund API health check completed"
            }
            
    except Exception as e:
        logger.error(f"Error in hedge fund API health check: {e}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
            "message": "Hedge fund API health check failed"
        } 