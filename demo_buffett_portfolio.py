#!/usr/bin/env python3
"""
Demo: Warren Buffett's Portfolio Analysis
Shows the working 13F functionality with real Berkshire Hathaway data.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from sec_13f.managers.berkshire_hathaway import get_buffett_latest_portfolio


async def demo_buffett_portfolio():
    """Demo Warren Buffett's latest portfolio."""
    
    print("🏦 Warren Buffett Portfolio Analysis")
    print("=" * 50)
    print("Fetching latest Berkshire Hathaway 13F filing...")
    print()
    
    try:
        portfolio = await get_buffett_latest_portfolio()
        
        if portfolio:
            print("📊 PORTFOLIO SUMMARY")
            print("-" * 30)
            print(f"📅 Filing Date: {portfolio.filing_date}")
            print(f"📅 Period End: {portfolio.period_end_date}")
            print(f"💰 Total Value: ${portfolio.total_value/1000:,.0f} billion")
            print(f"📈 Total Holdings: {portfolio.total_holdings_count}")
            print()
            
            print("🏆 TOP 10 HOLDINGS")
            print("-" * 30)
            top_10 = sorted(portfolio.holdings, key=lambda x: x.value, reverse=True)[:10]
            
            for i, holding in enumerate(top_10, 1):
                value_billions = holding.value / 1000
                print(f"{i:2d}. {holding.security_name}")
                print(f"     💵 ${value_billions:,.1f}B ({holding.value:,}K)")
                print(f"     📊 {holding.shares:,} shares")
                if holding.avg_price_per_share:
                    print(f"     📈 ~${holding.avg_price_per_share:.2f}/share")
                print()
            
            # Calculate concentration
            total_value = portfolio.total_value
            top_5_value = sum(h.value for h in top_10[:5])
            concentration = (top_5_value / total_value) * 100 if total_value > 0 else 0
            
            print("📊 PORTFOLIO METRICS")
            print("-" * 30)
            print(f"🎯 Top 5 Concentration: {concentration:.1f}%")
            print(f"🏢 Total Companies: {len(set(h.security_name for h in portfolio.holdings))}")
            print()
            
            print("✅ SUCCESS: 13F parsing is now working correctly!")
            print("🎉 Warren Buffett's portfolio data retrieved successfully!")
            
        else:
            print("❌ No portfolio data found")
            
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(demo_buffett_portfolio()) 