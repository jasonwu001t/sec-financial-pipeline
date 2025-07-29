"""
Berkshire Hathaway (Warren Buffett) Handler
Specialized functionality for analyzing Warren Buffett's investment decisions.
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from core.models import Form13F, HedgeFundManager, InstitutionalHolding, PortfolioAnalysis
# Import will be done locally to avoid circular imports


class BerkshireHathawayHandler:
    """Specialized handler for Berkshire Hathaway analysis."""
    
    CIK = "0001067983"
    MANAGER_NAME = "BERKSHIRE HATHAWAY INC"
    COMMON_NAME = "Berkshire Hathaway"
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.client = None
        
    async def __aenter__(self):
        """Async context manager entry."""
        # Import locally to avoid circular imports
        from sec_13f.client import SEC13FClient
        self.client = SEC13FClient()
        await self.client.start_session()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.client:
            await self.client.close_session()
    
    async def get_latest_portfolio(self) -> Optional[Form13F]:
        """Get Warren Buffett's latest 13F filing."""
        return await self.client.fetch_latest_13f_filing(self.CIK)
    
    async def get_portfolio_history(self, quarters: int = 8) -> List[Form13F]:
        """Get Berkshire's portfolio history over multiple quarters."""
        return await self.client.fetch_13f_filings(self.CIK, periods=quarters)
    
    async def analyze_buffett_picks(self, quarters: int = 4) -> Dict[str, Any]:
        """
        Analyze Warren Buffett's recent investment decisions.
        Focus on new positions, increased holdings, and exits.
        """
        filings = await self.get_portfolio_history(quarters)
        
        if len(filings) < 2:
            return {"error": "Need at least 2 quarters of data for analysis"}
        
        latest_filing = filings[0]
        previous_filing = filings[1]
        
        # Create CUSIP-based holdings lookup
        latest_holdings = {h.cusip: h for h in latest_filing.holdings}
        previous_holdings = {h.cusip: h for h in previous_filing.holdings}
        
        # Analyze changes
        new_positions = []
        increased_positions = []
        decreased_positions = []
        exited_positions = []
        
        # Find new positions (in latest but not in previous)
        for cusip, holding in latest_holdings.items():
            if cusip not in previous_holdings:
                new_positions.append(holding)
        
        # Find exits (in previous but not in latest)
        for cusip, holding in previous_holdings.items():
            if cusip not in latest_holdings:
                exited_positions.append(holding)
        
        # Find changes in existing positions
        for cusip, latest_holding in latest_holdings.items():
            if cusip in previous_holdings:
                previous_holding = previous_holdings[cusip]
                
                if latest_holding.shares > previous_holding.shares:
                    change_pct = ((latest_holding.shares - previous_holding.shares) / 
                                previous_holding.shares) * 100
                    increased_positions.append({
                        "holding": latest_holding,
                        "previous_shares": previous_holding.shares,
                        "current_shares": latest_holding.shares,
                        "change_percent": change_pct
                    })
                elif latest_holding.shares < previous_holding.shares:
                    change_pct = ((previous_holding.shares - latest_holding.shares) / 
                                previous_holding.shares) * 100
                    decreased_positions.append({
                        "holding": latest_holding,
                        "previous_shares": previous_holding.shares,
                        "current_shares": latest_holding.shares,
                        "change_percent": change_pct
                    })
        
        # Calculate top holdings concentration
        total_value = sum(h.value for h in latest_filing.holdings)
        top_10_value = sum(h.value for h in sorted(latest_filing.holdings, 
                                                 key=lambda x: x.value, reverse=True)[:10])
        concentration_ratio = (top_10_value / total_value) * 100 if total_value > 0 else 0
        
        analysis = {
            "analysis_date": datetime.utcnow().isoformat(),
            "period": f"{latest_filing.period_end_date} vs {previous_filing.period_end_date}",
            "total_portfolio_value": latest_filing.total_value,
            "total_holdings": latest_filing.total_holdings_count,
            "concentration_ratio": concentration_ratio,
            "portfolio_changes": {
                "new_positions": len(new_positions),
                "increased_positions": len(increased_positions),
                "decreased_positions": len(decreased_positions),
                "exited_positions": len(exited_positions)
            },
            "top_new_positions": sorted(new_positions, key=lambda x: x.value, reverse=True)[:5],
            "top_increased_positions": sorted(increased_positions, 
                                            key=lambda x: x["change_percent"], reverse=True)[:5],
            "top_decreased_positions": sorted(decreased_positions,
                                            key=lambda x: x["change_percent"], reverse=True)[:5],
            "exited_positions": sorted(exited_positions, key=lambda x: x.value, reverse=True)[:5],
            "top_holdings": sorted(latest_filing.holdings, key=lambda x: x.value, reverse=True)[:10]
        }
        
        return analysis
    
    async def get_sector_allocation(self) -> Dict[str, float]:
        """
        Get sector allocation of Berkshire's portfolio.
        Note: This would require mapping CUSIPs to sectors.
        """
        latest_portfolio = await self.get_latest_portfolio()
        if not latest_portfolio:
            return {}
        
        # For demonstration, we'll show the top holdings
        # In a real implementation, this would map CUSIPs to sectors
        top_holdings = sorted(latest_portfolio.holdings, key=lambda x: x.value, reverse=True)[:10]
        
        return {
            holding.security_name: (holding.value / latest_portfolio.total_value) * 100
            for holding in top_holdings
        }
    
    async def compare_to_market_benchmark(self, benchmark_tickers: List[str] = None) -> Dict[str, Any]:
        """
        Compare Berkshire's holdings to market benchmarks like S&P 500.
        This is a placeholder for more sophisticated analysis.
        """
        if benchmark_tickers is None:
            benchmark_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]  # Top S&P 500 stocks
        
        latest_portfolio = await self.get_latest_portfolio()
        if not latest_portfolio:
            return {"error": "No portfolio data available"}
        
        # Find overlap with benchmark
        portfolio_securities = {h.security_name.upper() for h in latest_portfolio.holdings}
        benchmark_overlap = []
        
        for holding in latest_portfolio.holdings:
            # This is simplified - would need proper ticker mapping
            for ticker in benchmark_tickers:
                if ticker in holding.security_name.upper():
                    benchmark_overlap.append({
                        "security": holding.security_name,
                        "ticker": ticker,
                        "value": holding.value,
                        "portfolio_percentage": (holding.value / latest_portfolio.total_value) * 100
                    })
        
        return {
            "analysis_date": datetime.utcnow().isoformat(),
            "total_benchmark_overlap": len(benchmark_overlap),
            "overlap_value": sum(item["value"] for item in benchmark_overlap),
            "overlap_percentage": sum(item["portfolio_percentage"] for item in benchmark_overlap),
            "benchmark_holdings": benchmark_overlap
        }
    
    async def get_berkshire_insights(self) -> Dict[str, Any]:
        """Get comprehensive insights into Berkshire Hathaway's investment strategy."""
        try:
            # Get multiple types of analysis
            portfolio_analysis = await self.analyze_buffett_picks()
            sector_allocation = await self.get_sector_allocation()
            benchmark_comparison = await self.compare_to_market_benchmark()
            
            latest_portfolio = await self.get_latest_portfolio()
            
            insights = {
                "manager_info": {
                    "name": self.COMMON_NAME,
                    "cik": self.CIK,
                    "key_personnel": ["Warren Buffett", "Charlie Munger"]
                },
                "portfolio_summary": {
                    "filing_date": latest_portfolio.filing_date.isoformat() if latest_portfolio else None,
                    "period_end_date": latest_portfolio.period_end_date.isoformat() if latest_portfolio else None,
                    "total_value": latest_portfolio.total_value if latest_portfolio else 0,
                    "total_holdings": latest_portfolio.total_holdings_count if latest_portfolio else 0
                },
                "investment_changes": portfolio_analysis.get("portfolio_changes", {}),
                "top_holdings": portfolio_analysis.get("top_holdings", []),
                "sector_allocation": sector_allocation,
                "market_benchmark_analysis": benchmark_comparison,
                "buffett_strategy_notes": [
                    "Focus on businesses with strong economic moats",
                    "Long-term value investing approach",
                    "Preference for companies with strong management",
                    "Typically holds positions for many years",
                    "Known for large, concentrated positions"
                ]
            }
            
            return insights
            
        except Exception as e:
            self.logger.error(f"Error generating Berkshire insights: {e}")
            return {"error": str(e)}


# Convenience functions for Berkshire Hathaway analysis

async def get_buffett_latest_portfolio() -> Optional[Form13F]:
    """Get Warren Buffett's latest portfolio."""
    async with BerkshireHathawayHandler() as handler:
        return await handler.get_latest_portfolio()


async def analyze_buffett_strategy(quarters: int = 4) -> Dict[str, Any]:
    """Analyze Warren Buffett's recent investment strategy."""
    async with BerkshireHathawayHandler() as handler:
        return await handler.analyze_buffett_picks(quarters)


async def get_berkshire_full_analysis() -> Dict[str, Any]:
    """Get comprehensive Berkshire Hathaway analysis."""
    async with BerkshireHathawayHandler() as handler:
        return await handler.get_berkshire_insights()


if __name__ == "__main__":
    # Test Berkshire Hathaway handler
    async def test():
        # Import locally for testing
        from sec_13f.client import SEC13FClient
        
        async with BerkshireHathawayHandler() as handler:
            print("Testing Berkshire Hathaway Handler...")
            
            try:
                # Get latest portfolio
                portfolio = await handler.get_latest_portfolio()
                if portfolio:
                    print(f"Latest filing: {portfolio.filing_date}")
                    print(f"Portfolio value: ${portfolio.total_value:,.0f}K")
                    print(f"Number of holdings: {portfolio.total_holdings_count}")
                    
                    # Show top 5 holdings
                    top_holdings = sorted(portfolio.holdings, key=lambda x: x.value, reverse=True)[:5]
                    print("\nTop 5 Holdings:")
                    for i, holding in enumerate(top_holdings, 1):
                        print(f"{i}. {holding.security_name}: ${holding.value:,.0f}K")
                else:
                    print("No portfolio data found")
                
                # Get comprehensive analysis
                print("\nGenerating comprehensive analysis...")
                insights = await handler.get_berkshire_insights()
                print(f"Analysis completed: {len(insights.get('top_holdings', []))} top holdings analyzed")
                
            except Exception as e:
                print(f"Error testing Berkshire handler: {e}")
    
    asyncio.run(test()) 