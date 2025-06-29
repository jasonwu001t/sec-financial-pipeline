"""
COMPREHENSIVE SEC FINANCIAL ANALYZER - ADVANCED EXAMPLES
═══════════════════════════════════════════════════════

This file demonstrates the full capabilities of the enhanced SEC financial analyzer.
"""

from main import *
import pandas as pd

def demonstrate_comprehensive_analysis():
    """Demonstrate all major features of the financial analyzer."""
    
    print("🏦 COMPREHENSIVE SEC FINANCIAL ANALYZER - FULL DEMO")
    print("=" * 70)
    
    # 1. COMPLETE FINANCIAL STATEMENTS
    print("\n📊 1. COMPLETE FINANCIAL STATEMENTS")
    print("-" * 50)
    
    company = "MSFT"
    print(f"Analyzing {company}...")
    
    # Income Statement
    print(f"\n{company} Income Statement (Annual, $ Millions):")
    income = get_income_statement(company, years=3)
    print((income / 1_000_000).round(1))
    
    # Balance Sheet
    print(f"\n{company} Balance Sheet (Annual, $ Millions):")
    balance = get_balance_sheet(company, years=3)
    print((balance / 1_000_000).round(1))
    
    # Cash Flow Statement
    print(f"\n{company} Cash Flow Statement (Annual, $ Millions):")
    cashflow = get_cash_flow_statement(company, years=3)
    print((cashflow / 1_000_000).round(1))
    
    # 2. FINANCIAL RATIOS & KEY METRICS
    print(f"\n💰 2. FINANCIAL RATIOS & KEY METRICS")
    print("-" * 50)
    ratios = calculate_financial_ratios(company, years=5)
    print(f"{company} Key Financial Metrics:")
    print(ratios.round(2))
    
    # 3. QUARTERLY ANALYSIS
    print(f"\n📅 3. QUARTERLY ANALYSIS")
    print("-" * 50)
    quarterly = get_quarterly_data(company, years=2)
    print(f"{company} Quarterly Trends ($ Millions):")
    print((quarterly / 1_000_000).round(1))
    
    # 4. INDUSTRY COMPARISON
    print(f"\n🔍 4. INDUSTRY COMPARISON")
    print("-" * 50)
    tech_giants = ["MSFT", "AAPL", "GOOGL", "AMZN"]
    
    # Revenue comparison
    revenue_comp = compare_companies(tech_giants, "revenue", years=3)
    print("Tech Giants Revenue Comparison ($ Billions):")
    print((revenue_comp / 1_000_000_000).round(1))
    
    # Net Income comparison
    print("\nTech Giants Net Income Comparison ($ Billions):")
    income_comp = compare_companies(tech_giants, "net_income", years=3)
    print((income_comp / 1_000_000_000).round(1))
    
    # 5. DETAILED RATIO ANALYSIS
    print(f"\n📈 5. DETAILED RATIO ANALYSIS")
    print("-" * 50)
    
    companies = ["MSFT", "AAPL", "GOOGL"]
    ratio_comparison = {}
    
    for comp in companies:
        try:
            ratios = calculate_financial_ratios(comp, years=1)
            if not ratios.empty:
                latest_year = ratios.index.max()
                ratio_comparison[comp] = ratios.loc[latest_year]
        except:
            continue
    
    if ratio_comparison:
        ratio_df = pd.DataFrame(ratio_comparison).T
        print("Latest Year Financial Ratios Comparison:")
        print(ratio_df[['revenue_millions', 'net_margin_%', 'roa_%', 'roe_%', 'eps_diluted']].round(2))


def analyze_specific_metrics():
    """Analyze specific financial metrics in detail."""
    
    print(f"\n🔬 SPECIFIC METRICS DEEP DIVE")
    print("-" * 50)
    
    # Profitability Analysis
    print("\nProfitability Metrics (Microsoft vs Apple):")
    companies = ["MSFT", "AAPL"]
    
    for company in companies:
        try:
            ratios = calculate_financial_ratios(company, years=3)
            if not ratios.empty:
                print(f"\n{company} Profitability Trends:")
                profit_metrics = ratios[['revenue_millions', 'gross_margin_%', 'operating_margin_%', 'net_margin_%']]
                print(profit_metrics.round(2))
        except Exception as e:
            print(f"Error analyzing {company}: {e}")
    
    # Cash Flow Analysis
    print(f"\nCash Flow Analysis - Operating vs Investing:")
    try:
        msft_cf = get_cash_flow_statement("MSFT", years=3)
        if not msft_cf.empty:
            cash_analysis = msft_cf.loc[['operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow']]
            print("Microsoft Cash Flows ($ Millions):")
            print((cash_analysis / 1_000_000).round(1))
    except Exception as e:
        print(f"Cash flow analysis error: {e}")


def quarterly_trends_analysis():
    """Analyze quarterly trends for growth patterns."""
    
    print(f"\n📊 QUARTERLY TRENDS ANALYSIS")
    print("-" * 50)
    
    companies = ["MSFT", "AAPL", "TSLA"]
    
    for company in companies:
        try:
            quarterly_data = get_quarterly_data(company, metrics=["revenue", "net_income"], years=2)
            if not quarterly_data.empty:
                print(f"\n{company} Quarterly Performance ($ Millions):")
                print((quarterly_data / 1_000_000).round(1))
                
                # Calculate quarter-over-quarter growth
                if "revenue" in quarterly_data.index:
                    revenue_row = quarterly_data.loc["revenue"]
                    revenue_growth = revenue_row.pct_change() * 100
                    print(f"{company} Revenue QoQ Growth (%):")
                    print(revenue_growth.round(2))
                    
        except Exception as e:
            print(f"Error analyzing {company} quarterly data: {e}")


def sector_analysis():
    """Perform sector-wide analysis."""
    
    print(f"\n🏭 SECTOR ANALYSIS")
    print("-" * 50)
    
    # Technology Sector
    tech_companies = ["MSFT", "AAPL", "GOOGL", "AMZN", "TSLA", "NVDA"]
    print("Technology Sector Analysis:")
    
    sector_metrics = {}
    
    for company in tech_companies:
        try:
            ratios = calculate_financial_ratios(company, years=1)
            if not ratios.empty:
                latest = ratios.iloc[-1]
                sector_metrics[company] = {
                    'Revenue (B)': latest['revenue_millions'] / 1000,
                    'Net Margin %': latest['net_margin_%'],
                    'ROE %': latest['roe_%'],
                    'ROA %': latest['roa_%']
                }
        except:
            continue
    
    if sector_metrics:
        sector_df = pd.DataFrame(sector_metrics).T
        print(sector_df.round(2))
        
        # Sector averages
        print(f"\nSector Averages:")
        print(f"Average Net Margin: {sector_df['Net Margin %'].mean():.2f}%")
        print(f"Average ROE: {sector_df['ROE %'].mean():.2f}%")
        print(f"Average ROA: {sector_df['ROA %'].mean():.2f}%")


if __name__ == "__main__":
    # Run comprehensive demonstrations
    demonstrate_comprehensive_analysis()
    analyze_specific_metrics()
    quarterly_trends_analysis()
    sector_analysis()
    
    print(f"\n✅ COMPREHENSIVE FINANCIAL ANALYSIS COMPLETE!")
    print("\n📚 AVAILABLE FINANCIAL STATEMENT ITEMS:")
    print(f"Income Statement: {len(INCOME_STATEMENT_ITEMS)} items")
    print(f"Balance Sheet: {len(BALANCE_SHEET_ITEMS)} items") 
    print(f"Cash Flow: {len(CASH_FLOW_ITEMS)} items")
    print(f"Other Metrics: {len(OTHER_METRICS)} items")
    
    print("\n🎯 KEY CAPABILITIES:")
    print("• Complete financial statements (Income, Balance Sheet, Cash Flow)")
    print("• Quarterly data from 10-Q filings")
    print("• Financial ratios and key performance indicators")
    print("• Multi-company comparisons")
    print("• Sector analysis")
    print("• Historical trend analysis")
    print("• Dividend and other important metrics")
    print("• All data sourced directly from SEC XBRL filings") 