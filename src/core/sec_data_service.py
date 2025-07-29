"""
Centralized SEC Data Service
Provides a unified interface for accessing different types of SEC filing data.
Supports company facts, 13F filings, and other SEC data types.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Union, Type
from datetime import datetime, date
from enum import Enum

from .sec_client import SECAPIClient
from .models import (
    CompanyInfo, FinancialFact, Form13F, HedgeFundManager, 
    InstitutionalHolding, FilingType
)


class SECDataType(str, Enum):
    """Types of SEC data supported."""
    COMPANY_FACTS = "company_facts"
    FORM_13F = "13f_filings"
    INSIDER_TRANSACTIONS = "insider_transactions"
    PROXY_STATEMENTS = "proxy_statements"


class SECDataService:
    """Centralized service for accessing different types of SEC data."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._company_facts_client = None
        self._form_13f_client = None
        
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._company_facts_client:
            await self._company_facts_client.close_session()
        if self._form_13f_client:
            await self._form_13f_client.close_session()
    
    @property
    async def company_facts_client(self) -> SECAPIClient:
        """Get or create company facts client."""
        if self._company_facts_client is None:
            self._company_facts_client = SECAPIClient()
            await self._company_facts_client.start_session()
        return self._company_facts_client
    
    @property  
    async def form_13f_client(self):
        """Get or create 13F client."""
        if self._form_13f_client is None:
            # Import here to avoid circular imports
            from ..sec_13f.client import SEC13FClient
            self._form_13f_client = SEC13FClient()
            await self._form_13f_client.start_session()
        return self._form_13f_client
    
    # Company Facts Methods
    
    async def get_company_facts(self, ticker: str) -> tuple[CompanyInfo, List[FinancialFact]]:
        """Get company facts data for a ticker."""
        client = await self.company_facts_client
        return await client.fetch_company_data(ticker)
    
    async def get_multiple_company_facts(self, tickers: List[str]) -> Dict[str, tuple[CompanyInfo, List[FinancialFact]]]:
        """Get company facts for multiple tickers."""
        client = await self.company_facts_client
        return await client.batch_fetch_companies(tickers)
    
    # 13F Filing Methods
    
    async def get_13f_filings(self, cik: str, periods: Optional[int] = 4) -> List[Form13F]:
        """Get 13F filings for an institutional manager."""
        client = await self.form_13f_client
        return await client.fetch_13f_filings(cik, periods)
    
    async def get_latest_13f_filing(self, cik: str) -> Optional[Form13F]:
        """Get the most recent 13F filing for an institutional manager."""
        client = await self.form_13f_client
        return await client.fetch_latest_13f_filing(cik)
    
    async def get_hedge_fund_managers(self) -> List[HedgeFundManager]:
        """Get list of famous hedge fund managers."""
        # Import locally to avoid circular imports
        import importlib.util
        from pathlib import Path
        
        managers_file = Path(__file__).parent.parent / "sec_13f" / "managers.py"
        spec = importlib.util.spec_from_file_location("managers_data", managers_file)
        managers_data = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(managers_data)
        
        return managers_data.get_famous_hedge_fund_managers()
    
    async def get_manager_by_cik(self, cik: str) -> Optional[HedgeFundManager]:
        """Get hedge fund manager information by CIK."""
        client = await self.form_13f_client
        return await client.get_manager_by_cik(cik)
    
    # Cross-data type analysis
    
    async def analyze_manager_holdings_vs_company_performance(
        self, 
        manager_cik: str, 
        periods: int = 4
    ) -> Dict[str, Any]:
        """
        Analyze hedge fund holdings against company performance.
        Combines 13F data with company facts for comprehensive analysis.
        """
        # Get 13F filings
        filings = await self.get_13f_filings(manager_cik, periods)
        
        if not filings:
            return {"error": "No 13F filings found for this manager"}
        
        # Extract unique tickers from holdings
        all_cusips = set()
        ticker_to_cusip = {}
        
        for filing in filings:
            for holding in filing.holdings:
                all_cusips.add(holding.cusip)
                # We'll need a CUSIP to ticker mapping service
                # For now, store the mapping if we have it
                
        # Get company performance data for major holdings
        analysis_results = {
            "manager_cik": manager_cik,
            "analysis_period": f"{len(filings)} quarters",
            "total_filings_analyzed": len(filings),
            "holdings_analysis": [],
            "performance_correlation": None  # To be implemented
        }
        
        # Analyze each major holding
        for filing in filings[-1:]:  # Start with latest filing
            for holding in filing.holdings[:10]:  # Top 10 holdings
                try:
                    # This would require CUSIP to ticker mapping
                    # For now, we'll structure the data for future implementation
                    holding_analysis = {
                        "cusip": holding.cusip,
                        "security_name": holding.security_name,
                        "current_value": holding.value,
                        "shares": holding.shares,
                        "portfolio_percentage": holding.percentage_of_portfolio,
                        "company_performance": None  # To be filled with company facts
                    }
                    analysis_results["holdings_analysis"].append(holding_analysis)
                    
                except Exception as e:
                    self.logger.warning(f"Failed to analyze holding {holding.cusip}: {e}")
                    continue
        
        return analysis_results
    
    # Data validation and health checks
    
    async def validate_data_source(self, data_type: SECDataType) -> Dict[str, Any]:
        """Validate that a data source is accessible and working."""
        validation_result = {
            "data_type": data_type,
            "status": "unknown",
            "last_checked": datetime.utcnow(),
            "error": None
        }
        
        try:
            if data_type == SECDataType.COMPANY_FACTS:
                # Test with a known ticker
                client = await self.company_facts_client
                await client.get_cik_for_ticker("AAPL")
                validation_result["status"] = "healthy"
                
            elif data_type == SECDataType.FORM_13F:
                # Test with a known CIK (Berkshire Hathaway)
                client = await self.form_13f_client
                await client.validate_cik("0001067983")
                validation_result["status"] = "healthy"
                
        except Exception as e:
            validation_result["status"] = "error"
            validation_result["error"] = str(e)
            
        return validation_result
    
    async def get_service_health(self) -> Dict[str, Any]:
        """Get overall health status of all SEC data services."""
        health_checks = []
        
        for data_type in SECDataType:
            try:
                result = await self.validate_data_source(data_type)
                health_checks.append(result)
            except Exception as e:
                health_checks.append({
                    "data_type": data_type,
                    "status": "error",
                    "error": str(e),
                    "last_checked": datetime.utcnow()
                })
        
        overall_status = "healthy" if all(
            check["status"] == "healthy" for check in health_checks
        ) else "degraded"
        
        return {
            "overall_status": overall_status,
            "last_checked": datetime.utcnow(),
            "services": health_checks
        }


# Convenience functions for external use

async def get_company_data(ticker: str) -> tuple[CompanyInfo, List[FinancialFact]]:
    """Convenience function to get company facts data."""
    async with SECDataService() as service:
        return await service.get_company_facts(ticker)


async def get_hedge_fund_portfolio(cik: str, periods: int = 4) -> List[Form13F]:
    """Convenience function to get 13F portfolio data."""
    async with SECDataService() as service:
        return await service.get_13f_filings(cik, periods)


async def get_famous_hedge_funds() -> List[HedgeFundManager]:
    """Convenience function to get list of famous hedge fund managers."""
    async with SECDataService() as service:
        return await service.get_hedge_fund_managers()


if __name__ == "__main__":
    # Test the service
    async def test():
        async with SECDataService() as service:
            # Test company facts
            print("Testing company facts...")
            company_info, facts = await service.get_company_facts("AAPL")
            print(f"Fetched {len(facts)} facts for {company_info.name}")
            
            # Test service health
            print("\nTesting service health...")
            health = await service.get_service_health()
            print(f"Overall status: {health['overall_status']}")
            
            # Test famous hedge funds (will work once we implement the 13F client)
            print("\nTesting hedge fund managers...")
            try:
                managers = await service.get_hedge_fund_managers()
                print(f"Found {len(managers)} hedge fund managers")
            except Exception as e:
                print(f"13F functionality not yet implemented: {e}")
    
    asyncio.run(test()) 