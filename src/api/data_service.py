"""
Data Service for SEC Financial Data API.
Provides business logic and data transformation for API endpoints.
"""

import logging
import pandas as pd
import io
from typing import List, Dict, Any, Optional
from datetime import datetime

from core.models import CompanyData, FinancialFact, ReportingPeriod
from etl.data_manager import DataManager
from .cache import CacheManager


class DataService:
    """Service layer for financial data operations."""
    
    def __init__(self, data_manager: DataManager, cache_manager: CacheManager):
        self.data_manager = data_manager
        self.cache_manager = cache_manager
        self.logger = logging.getLogger(__name__)
        
        # Common financial metrics mapping
        self.metric_mappings = {
            "revenue": ["Revenues", "Revenue", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"],
            "net_income": ["NetIncomeLoss", "NetIncome", "ProfitLoss"],
            "total_assets": ["Assets", "AssetsCurrent", "AssetsTotal"],
            "total_liabilities": ["Liabilities", "LiabilitiesTotal", "LiabilitiesCurrent"],
            "cash": ["Cash", "CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsAndShortTermInvestments"],
            "shareholders_equity": ["StockholdersEquity", "ShareholdersEquity", "EquityAttributableToParent"],
            "earnings_per_share": ["EarningsPerShareBasic", "EarningsPerShareDiluted"],
            "operating_income": ["OperatingIncomeLoss", "IncomeFromOperations"],
            "gross_profit": ["GrossProfit"],
            "research_development": ["ResearchAndDevelopmentExpense"],
            "debt": ["DebtCurrent", "LongTermDebt", "DebtTotal"],
        }
    
    async def get_company_data(self, ticker: str, years: Optional[int] = None) -> Optional[CompanyData]:
        """Get company data with caching."""
        ticker = ticker.upper()
        years = years or 5
        
        # Try cache first
        cached_data = await self.cache_manager.get_company_data(ticker, years)
        if cached_data:
            return cached_data
        
        # Load from data manager
        company_data = self.data_manager.load_company_data(ticker, years)
        
        if company_data:
            # Cache the result
            await self.cache_manager.set_company_data(ticker, company_data, years)
        
        return company_data
    
    async def get_metric_data(self, ticker: str, metric: str, period: ReportingPeriod, years: int) -> Optional[List[Dict[str, Any]]]:
        """Get specific metric data for a ticker."""
        ticker = ticker.upper()
        period_str = period.value
        
        # Try cache first
        cached_data = await self.cache_manager.get_metric_data(ticker, metric, period_str, years)
        if cached_data:
            return cached_data
        
        # Get company data
        company_data = await self.get_company_data(ticker, years)
        if not company_data:
            return None
        
        # Extract metric data
        metric_data = self._extract_metric_from_facts(
            company_data.raw_facts, metric, period, years
        )
        
        if metric_data:
            # Cache the result
            await self.cache_manager.set_metric_data(ticker, metric, period_str, years, metric_data)
        
        return metric_data
    
    async def compare_companies(self, tickers: List[str], metric: str, period: ReportingPeriod, years: int) -> List[Dict[str, Any]]:
        """Compare a metric across multiple companies."""
        tickers = [t.upper() for t in tickers]
        period_str = period.value
        
        # Try cache first
        cached_data = await self.cache_manager.get_comparison_data(tickers, metric, period_str, years)
        if cached_data:
            return cached_data
        
        comparison_data = []
        
        for ticker in tickers:
            metric_data = await self.get_metric_data(ticker, metric, period, years)
            if metric_data:
                comparison_data.append({
                    "ticker": ticker,
                    "metric": metric,
                    "data": metric_data
                })
        
        if comparison_data:
            # Cache the result
            await self.cache_manager.set_comparison_data(tickers, metric, period_str, years, comparison_data)
        
        return comparison_data
    
    def _extract_metric_from_facts(self, facts: List[FinancialFact], metric: str, period: ReportingPeriod, years: int) -> List[Dict[str, Any]]:
        """Extract specific metric data from financial facts."""
        # Get possible labels for this metric
        possible_labels = self.metric_mappings.get(metric.lower(), [metric])
        
        # Filter facts by metric labels
        relevant_facts = []
        for fact in facts:
            if any(label.lower() in fact.label.lower() for label in possible_labels):
                relevant_facts.append(fact)
        
        if not relevant_facts:
            return []
        
        # Group by fiscal year and period
        grouped_data = {}
        
        for fact in relevant_facts:
            if not fact.fiscal_year:
                continue
            
            # Filter by period type
            is_annual = fact.fiscal_period in [None, "FY"]
            is_quarterly = fact.fiscal_period and fact.fiscal_period.startswith("Q")
            
            if period == ReportingPeriod.ANNUAL and not is_annual:
                continue
            elif period == ReportingPeriod.QUARTERLY and not is_quarterly:
                continue
            
            # Create key for grouping
            key = f"{fact.fiscal_year}"
            if fact.fiscal_period and fact.fiscal_period != "FY":
                key += f"_{fact.fiscal_period}"
            
            if key not in grouped_data:
                grouped_data[key] = {
                    "fiscal_year": fact.fiscal_year,
                    "fiscal_period": fact.fiscal_period,
                    "end_date": fact.end_date,
                    "values": []
                }
            
            grouped_data[key]["values"].append({
                "value": fact.value,
                "label": fact.label,
                "unit": fact.unit.value if fact.unit else None,
                "form": fact.form.value if fact.form else None
            })
        
        # Convert to list and sort
        result = []
        for key, data in grouped_data.items():
            # Take the most recent/largest value if multiple values exist
            if data["values"]:
                best_value = max(data["values"], key=lambda x: abs(x["value"]) if x["value"] else 0)
                result.append({
                    "fiscal_year": data["fiscal_year"],
                    "fiscal_period": data["fiscal_period"],
                    "end_date": data["end_date"].isoformat() if data["end_date"] else None,
                    "value": best_value["value"],
                    "label": best_value["label"],
                    "unit": best_value["unit"],
                    "form": best_value["form"]
                })
        
        # Sort by fiscal year (descending) and limit to requested years
        result.sort(key=lambda x: (x["fiscal_year"], x["fiscal_period"] or ""), reverse=True)
        
        # Filter by years
        current_year = datetime.now().year
        min_year = current_year - years + 1
        result = [r for r in result if r["fiscal_year"] >= min_year]
        
        return result[:years * (4 if period == ReportingPeriod.QUARTERLY else 1)]
    
    def convert_to_csv(self, company_data: CompanyData, period: ReportingPeriod) -> str:
        """Convert company data to CSV format."""
        try:
            # Create DataFrame from facts
            facts_data = []
            
            for fact in company_data.raw_facts:
                # Filter by period
                is_annual = fact.fiscal_period in [None, "FY"]
                is_quarterly = fact.fiscal_period and fact.fiscal_period.startswith("Q")
                
                if period == ReportingPeriod.ANNUAL and not is_annual:
                    continue
                elif period == ReportingPeriod.QUARTERLY and not is_quarterly:
                    continue
                
                facts_data.append({
                    "ticker": company_data.company_info.ticker,
                    "company_name": company_data.company_info.name,
                    "fiscal_year": fact.fiscal_year,
                    "fiscal_period": fact.fiscal_period,
                    "label": fact.label,
                    "value": fact.value,
                    "unit": fact.unit.value if fact.unit else None,
                    "start_date": fact.start_date.isoformat() if fact.start_date else None,
                    "end_date": fact.end_date.isoformat() if fact.end_date else None,
                    "instant_date": fact.instant_date.isoformat() if fact.instant_date else None,
                    "form": fact.form.value if fact.form else None
                })
            
            if not facts_data:
                return "ticker,message\n" + f"{company_data.company_info.ticker},No data available for selected period"
            
            df = pd.DataFrame(facts_data)
            return df.to_csv(index=False)
            
        except Exception as e:
            self.logger.error(f"Error converting to CSV: {e}")
            return f"ticker,error\n{company_data.company_info.ticker},Error generating CSV"
    
    def convert_to_parquet(self, company_data: CompanyData, period: ReportingPeriod) -> bytes:
        """Convert company data to Parquet format."""
        try:
            # Create DataFrame from facts (similar to CSV conversion)
            facts_data = []
            
            for fact in company_data.raw_facts:
                # Filter by period
                is_annual = fact.fiscal_period in [None, "FY"]
                is_quarterly = fact.fiscal_period and fact.fiscal_period.startswith("Q")
                
                if period == ReportingPeriod.ANNUAL and not is_annual:
                    continue
                elif period == ReportingPeriod.QUARTERLY and not is_quarterly:
                    continue
                
                facts_data.append({
                    "ticker": company_data.company_info.ticker,
                    "company_name": company_data.company_info.name,
                    "fiscal_year": fact.fiscal_year,
                    "fiscal_period": fact.fiscal_period,
                    "label": fact.label,
                    "value": fact.value,
                    "unit": fact.unit.value if fact.unit else None,
                    "start_date": fact.start_date.isoformat() if fact.start_date else None,
                    "end_date": fact.end_date.isoformat() if fact.end_date else None,
                    "instant_date": fact.instant_date.isoformat() if fact.instant_date else None,
                    "form": fact.form.value if fact.form else None
                })
            
            df = pd.DataFrame(facts_data)
            
            # Convert to parquet bytes
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, compression='snappy')
            return buffer.getvalue()
            
        except Exception as e:
            self.logger.error(f"Error converting to Parquet: {e}")
            # Return empty parquet file
            empty_df = pd.DataFrame({"ticker": [company_data.company_info.ticker], "error": ["Error generating Parquet"]})
            buffer = io.BytesIO()
            empty_df.to_parquet(buffer, index=False)
            return buffer.getvalue()
    
    def convert_metric_to_csv(self, metric_data: List[Dict[str, Any]], ticker: str, metric: str) -> str:
        """Convert metric data to CSV format."""
        try:
            if not metric_data:
                return f"ticker,metric,message\n{ticker},{metric},No data available"
            
            # Convert to DataFrame
            df_data = []
            for item in metric_data:
                df_data.append({
                    "ticker": ticker,
                    "metric": metric,
                    "fiscal_year": item.get("fiscal_year"),
                    "fiscal_period": item.get("fiscal_period"),
                    "end_date": item.get("end_date"),
                    "value": item.get("value"),
                    "label": item.get("label"),
                    "unit": item.get("unit"),
                    "form": item.get("form")
                })
            
            df = pd.DataFrame(df_data)
            return df.to_csv(index=False)
            
        except Exception as e:
            self.logger.error(f"Error converting metric to CSV: {e}")
            return f"ticker,metric,error\n{ticker},{metric},Error generating CSV"
    
    def get_available_metrics(self) -> List[Dict[str, Any]]:
        """Get list of available financial metrics."""
        return [
            {
                "metric": key,
                "description": f"Financial metric: {key.replace('_', ' ').title()}",
                "possible_labels": labels
            }
            for key, labels in self.metric_mappings.items()
        ]
    
    def validate_ticker_format(self, ticker: str) -> bool:
        """Validate ticker format."""
        if not ticker or not isinstance(ticker, str):
            return False
        
        ticker = ticker.upper().strip()
        
        # Basic validation - alphanumeric and dots, 1-5 characters
        if not (1 <= len(ticker) <= 6):
            return False
        
        # Allow letters, numbers, and dots (for class shares like BRK.B)
        return ticker.replace(".", "").replace("-", "").isalnum()
    
    def get_data_summary(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get a summary of available data for a ticker."""
        ticker = ticker.upper()
        
        freshness = self.data_manager.get_data_freshness(ticker)
        if not freshness:
            return None
        
        file_info = self.data_manager.get_ticker_file_info(ticker)
        
        # Calculate coverage
        annual_coverage = len(freshness.annual_data_years)
        quarterly_coverage = len(freshness.quarterly_data_periods)
        
        return {
            "ticker": ticker,
            "last_updated": freshness.last_updated,
            "last_filing_date": freshness.last_sec_filing_date,
            "annual_years_available": freshness.annual_data_years,
            "quarterly_periods_available": len(freshness.quarterly_data_periods),
            "total_files": len(file_info),
            "total_records": sum(f.record_count or 0 for f in file_info),
            "coverage": {
                "annual_years": annual_coverage,
                "quarterly_periods": quarterly_coverage,
                "oldest_year": min(freshness.annual_data_years) if freshness.annual_data_years else None,
                "newest_year": max(freshness.annual_data_years) if freshness.annual_data_years else None
            }
        }


if __name__ == "__main__":
    # Test the data service
    from etl.data_manager import DataManager
    from .cache import CacheManager
    import asyncio
    
    async def test_service():
        data_manager = DataManager()
        cache_manager = CacheManager()
        await cache_manager.initialize()
        
        service = DataService(data_manager, cache_manager)
        
        # Test available metrics
        metrics = service.get_available_metrics()
        print(f"Available metrics: {len(metrics)}")
        
        # Test ticker validation
        print(f"AAPL valid: {service.validate_ticker_format('AAPL')}")
        print(f"BRK.B valid: {service.validate_ticker_format('BRK.B')}")
        print(f"Invalid ticker: {service.validate_ticker_format('TOOLONG')}")
        
        await cache_manager.close()
    
    asyncio.run(test_service()) 