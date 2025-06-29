"""
Comprehensive SEC Financial Analyzer
────────────────────────────────────
• Complete financial statements: Income Statement, Balance Sheet, Cash Flow
• Quarterly and annual data from 10-K/10-Q filings
• Financial ratios and key performance indicators
• Dividend analysis and other important metrics
"""

from __future__ import annotations
import datetime as dt
import json
from functools import lru_cache
from typing import Dict, Any, Optional, List
import pandas as pd
import requests
import numpy as np

# ─── Constants ──────────────────────────────────────────────────────────────────
_USER_AGENT = (
    "comprehensive-financial-analyzer/1.0 "
    "(Contact: jasonwu001t@gmail.com) "
    "- https://www.sec.gov - compliant UA per SEC policy"
)
_TICKERS_JSON = "https://www.sec.gov/files/company_tickers.json"
_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# ─── Financial Statement Item Mappings ─────────────────────────────────────────
# Each item maps to a list of possible XBRL tags (in order of preference)

INCOME_STATEMENT_ITEMS = {
    # Revenue & Sales
    "revenue": ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues", "SalesRevenueNet"],
    "cost_of_revenue": ["CostOfRevenue", "CostOfGoodsAndServicesSold"],
    "gross_profit": ["GrossProfit"],
    
    # Operating Expenses
    "research_development": ["ResearchAndDevelopmentExpense"],
    "sales_marketing": ["SellingAndMarketingExpense", "AdvertisingExpense"],
    "general_administrative": ["GeneralAndAdministrativeExpense"],
    "total_operating_expenses": ["OperatingExpenses"],
    
    # Income Items
    "operating_income": ["OperatingIncomeLoss"],
    "interest_income": ["InterestIncomeOperating", "InvestmentIncomeInterest"],
    "interest_expense": ["InterestExpense"],
    "other_income": ["OtherNonoperatingIncomeExpense", "NonoperatingIncomeExpense"],
    "pretax_income": ["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinary"],
    "income_tax": ["IncomeTaxExpenseBenefit"],
    "net_income": ["NetIncomeLoss", "ProfitLoss"],
    
    # Per Share Data
    "eps_basic": ["EarningsPerShareBasic"],
    "eps_diluted": ["EarningsPerShareDiluted"],
    "shares_basic": ["WeightedAverageNumberOfSharesOutstandingBasic"],
    "shares_diluted": ["WeightedAverageNumberOfDilutedSharesOutstanding"],
}

BALANCE_SHEET_ITEMS = {
    # Current Assets
    "cash": ["CashAndCashEquivalentsAtCarryingValue", "Cash"],
    "short_term_investments": ["ShortTermInvestments", "MarketableSecuritiesCurrent"],
    "accounts_receivable": ["AccountsReceivableNetCurrent"],
    "inventory": ["InventoryNet"],
    "prepaid_expenses": ["PrepaidExpenseAndOtherAssetsCurrent"],
    "total_current_assets": ["AssetsCurrent"],
    
    # Non-Current Assets
    "property_plant_equipment": ["PropertyPlantAndEquipmentNet"],
    "goodwill": ["Goodwill"],
    "intangible_assets": ["IntangibleAssetsNetExcludingGoodwill"],
    "long_term_investments": ["LongTermInvestments", "MarketableSecuritiesNoncurrent"],
    "other_assets": ["OtherAssetsNoncurrent"],
    "total_assets": ["Assets"],
    
    # Current Liabilities
    "accounts_payable": ["AccountsPayableCurrent"],
    "accrued_liabilities": ["AccruedLiabilitiesCurrent"],
    "short_term_debt": ["ShortTermBorrowings", "DebtCurrent"],
    "deferred_revenue_current": ["DeferredRevenueCurrent"],
    "total_current_liabilities": ["LiabilitiesCurrent"],
    
    # Non-Current Liabilities
    "long_term_debt": ["LongTermDebtNoncurrent"],
    "deferred_revenue_noncurrent": ["DeferredRevenueNoncurrent"],
    "other_liabilities": ["OtherLiabilitiesNoncurrent"],
    "total_liabilities": ["Liabilities"],
    
    # Shareholders' Equity
    "common_stock": ["CommonStockValue"],
    "retained_earnings": ["RetainedEarningsAccumulatedDeficit"],
    "accumulated_other_comprehensive": ["AccumulatedOtherComprehensiveIncomeLossNetOfTax"],
    "total_equity": ["StockholdersEquity"],
}

CASH_FLOW_ITEMS = {
    # Operating Activities
    "net_income_cf": ["NetIncomeLoss"],
    "depreciation": ["DepreciationDepletionAndAmortization", "Depreciation"],
    "amortization": ["AmortizationOfIntangibleAssets"],
    "stock_compensation": ["ShareBasedCompensation"],
    "operating_cash_flow": ["NetCashProvidedByUsedInOperatingActivities"],
    
    # Investing Activities
    "capital_expenditures": ["PaymentsToAcquirePropertyPlantAndEquipment"],
    "acquisitions": ["PaymentsToAcquireBusinessesNetOfCashAcquired"],
    "investments_purchased": ["PaymentsToAcquireInvestments"],
    "investments_sold": ["ProceedsFromSaleMaturityAndCollectionOfInvestments"],
    "investing_cash_flow": ["NetCashProvidedByUsedInInvestingActivities"],
    
    # Financing Activities
    "dividends_paid": ["PaymentsOfDividends"],
    "stock_repurchased": ["PaymentsForRepurchaseOfCommonStock"],
    "stock_issued": ["ProceedsFromIssuanceOfCommonStock"],
    "debt_issued": ["ProceedsFromIssuanceOfLongTermDebt"],
    "debt_repaid": ["RepaymentsOfLongTermDebt"],
    "financing_cash_flow": ["NetCashProvidedByUsedInFinancingActivities"],
    
    # Net Change
    "net_change_cash": ["CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect"],
}

OTHER_METRICS = {
    "dividends_per_share": ["CommonStockDividendsPerShareDeclared"],
    "book_value_per_share": ["BookValuePerShare"],
}

# ─── Helper Functions ──────────────────────────────────────────────────────────
@lru_cache(maxsize=1)
def _ticker_to_cik() -> dict[str, str]:
    """Download SEC's master ticker list once and cache it."""
    resp = requests.get(_TICKERS_JSON, headers={"User-Agent": _USER_AGENT}, timeout=30)
    resp.raise_for_status()
    raw = resp.json()
    return {row["ticker"].upper(): str(row["cik_str"]).zfill(10) for row in raw.values()}


def _find_best_tag(tag_options: List[str], facts: dict) -> str:
    """Find the first available tag from a list of options."""
    for tag in tag_options:
        if tag in facts:
            return tag
    return tag_options[0]  # Return first option as fallback


def _pick_preferred_unit(unit_dict: dict) -> list:
    """Pick the preferred unit (USD for financial data, shares for share counts)."""
    if "USD" in unit_dict:
        return unit_dict["USD"]
    elif "shares" in unit_dict:
        return unit_dict["shares"]
    else:
        return next(iter(unit_dict.values()))


class FinancialAnalyzer:
    """Comprehensive financial analyzer for SEC data."""
    
    def __init__(self, company: str):
        """Initialize with company ticker or CIK."""
        self.company = company.upper()
        self.cik = self._resolve_cik(company)
        self.facts = self._fetch_company_facts()
        self.entity_name = self._get_entity_name()
        
    def _resolve_cik(self, company: str) -> str:
        """Resolve company ticker to CIK."""
        if company.isdigit():
            return company.zfill(10)
        
        mapping = _ticker_to_cik()
        cik = mapping.get(company.upper())
        if cik is None:
            raise ValueError(f"Unknown ticker {company!r}")
        return cik
    
    def _fetch_company_facts(self) -> dict:
        """Fetch company facts from SEC API."""
        url = _FACTS_URL.format(cik=self.cik)
        print(f"Fetching financial data for {self.company}...")
        
        resp = requests.get(url, headers={"User-Agent": _USER_AGENT}, timeout=30)
        if resp.status_code != 200:
            raise ValueError(f"SEC API returned {resp.status_code} for CIK {self.cik}")
        
        data = resp.json()
        self._raw_facts = data  # Store full response for entity info
        return data["facts"]["us-gaap"]
    
    def _get_entity_name(self) -> str:
        """Get the official entity name."""
        return self._raw_facts.get("entityName", self.company)
    
    def _extract_financial_data(
        self, 
        items_mapping: Dict[str, List[str]], 
        period: str = "annual",
        years: int = 5
    ) -> Dict[str, Dict]:
        """Extract financial data for specified items."""
        end_year = dt.datetime.now().year
        start_year = end_year - years + 1
        
        data = {}
        
        for friendly_name, tag_options in items_mapping.items():
            best_tag = _find_best_tag(tag_options, self.facts)
            
            if best_tag not in self.facts:
                data[friendly_name] = {}
                continue
                
            entries = _pick_preferred_unit(self.facts[best_tag]["units"])
            yearly_data = {}
            
            for item in entries:
                fy = item.get("fy")
                fp = item.get("fp", "")
                
                if fy is None or fy < start_year or fy > end_year:
                    continue
                    
                # Period filter
                if period == "annual" and not fp.startswith("FY"):
                    continue
                elif period == "quarterly" and fp.startswith("FY"):
                    continue
                
                # Create appropriate key
                if period == "quarterly":
                    key = f"{fy}-{fp}"
                    yearly_data[key] = item["val"]
                else:
                    yearly_data[fy] = item["val"]
                    
            data[friendly_name] = yearly_data
        
        return data
    
    def get_income_statement(self, period: str = "annual", years: int = 5) -> pd.DataFrame:
        """Get comprehensive income statement."""
        data = self._extract_financial_data(INCOME_STATEMENT_ITEMS, period, years)
        
        df = pd.DataFrame(data).T
        df = df.sort_index(axis=1)
        df.index.name = "Income Statement Item"
        
        return df
    
    def get_balance_sheet(self, period: str = "annual", years: int = 5) -> pd.DataFrame:
        """Get comprehensive balance sheet."""
        data = self._extract_financial_data(BALANCE_SHEET_ITEMS, period, years)
        
        df = pd.DataFrame(data).T
        df = df.sort_index(axis=1)
        df.index.name = "Balance Sheet Item"
        
        return df
    
    def get_cash_flow_statement(self, period: str = "annual", years: int = 5) -> pd.DataFrame:
        """Get comprehensive cash flow statement."""
        data = self._extract_financial_data(CASH_FLOW_ITEMS, period, years)
        
        df = pd.DataFrame(data).T
        df = df.sort_index(axis=1)
        df.index.name = "Cash Flow Item"
        
        return df
    
    def get_key_metrics(self, years: int = 5) -> pd.DataFrame:
        """Calculate key financial metrics and ratios."""
        # Get the underlying data
        income_data = self._extract_financial_data(INCOME_STATEMENT_ITEMS, "annual", years)
        balance_data = self._extract_financial_data(BALANCE_SHEET_ITEMS, "annual", years)
        other_data = self._extract_financial_data(OTHER_METRICS, "annual", years)
        
        metrics = {}
        
        # Get all available years
        all_years = set()
        for item_data in income_data.values():
            all_years.update(item_data.keys())
        for item_data in balance_data.values():
            all_years.update(item_data.keys())
            
        for year in sorted(all_years):
            if not isinstance(year, int):
                continue
                
            year_metrics = {}
            
            # Get base values
            revenue = income_data.get("revenue", {}).get(year, 0)
            gross_profit = income_data.get("gross_profit", {}).get(year, 0)
            operating_income = income_data.get("operating_income", {}).get(year, 0)
            net_income = income_data.get("net_income", {}).get(year, 0)
            total_assets = balance_data.get("total_assets", {}).get(year, 0)
            total_equity = balance_data.get("total_equity", {}).get(year, 0)
            
            # Calculate margins
            if revenue > 0:
                year_metrics["revenue_millions"] = revenue / 1_000_000
                year_metrics["gross_margin_%"] = (gross_profit / revenue) * 100
                year_metrics["operating_margin_%"] = (operating_income / revenue) * 100
                year_metrics["net_margin_%"] = (net_income / revenue) * 100
            
            # Calculate returns
            if total_assets > 0:
                year_metrics["roa_%"] = (net_income / total_assets) * 100
            
            if total_equity > 0:
                year_metrics["roe_%"] = (net_income / total_equity) * 100
            
            # EPS and other metrics
            year_metrics["eps_basic"] = income_data.get("eps_basic", {}).get(year, 0)
            year_metrics["eps_diluted"] = income_data.get("eps_diluted", {}).get(year, 0)
            year_metrics["dividends_per_share"] = other_data.get("dividends_per_share", {}).get(year, 0)
            
            # Balance sheet metrics
            year_metrics["total_assets_millions"] = total_assets / 1_000_000 if total_assets else 0
            year_metrics["total_equity_millions"] = total_equity / 1_000_000 if total_equity else 0
            
            metrics[year] = year_metrics
        
        df = pd.DataFrame(metrics).T
        df.index.name = "Year"
        
        return df
    
    def get_quarterly_data(self, years: int = 2) -> pd.DataFrame:
        """Get quarterly data for key metrics."""
        # Key quarterly items
        quarterly_items = {
            "revenue": INCOME_STATEMENT_ITEMS["revenue"],
            "net_income": INCOME_STATEMENT_ITEMS["net_income"],
            "eps_diluted": INCOME_STATEMENT_ITEMS["eps_diluted"],
        }
        
        data = self._extract_financial_data(quarterly_items, "quarterly", years)
        
        df = pd.DataFrame(data).T
        df = df.sort_index(axis=1)
        df.index.name = "Quarterly Metric"
        
        return df
    
    def generate_report(self) -> str:
        """Generate comprehensive financial analysis report."""
        report = []
        report.append(f"FINANCIAL ANALYSIS REPORT")
        report.append(f"Company: {self.entity_name} ({self.company})")
        report.append(f"CIK: {self.cik}")
        report.append("=" * 80)
        report.append("")
        
        try:
            # Key Metrics Summary
            report.append("KEY FINANCIAL METRICS (Last 5 Years)")
            report.append("-" * 50)
            metrics = self.get_key_metrics()
            if not metrics.empty:
                report.append(metrics.round(2).to_string())
            else:
                report.append("No key metrics data available")
            report.append("")
            
            # Income Statement
            report.append("INCOME STATEMENT - Annual ($ Millions)")
            report.append("-" * 50)
            income = self.get_income_statement()
            if not income.empty:
                income_millions = income / 1_000_000
                report.append(income_millions.round(1).to_string())
            else:
                report.append("No income statement data available")
            report.append("")
            
            # Balance Sheet
            report.append("BALANCE SHEET - Annual ($ Millions)")
            report.append("-" * 50)
            balance = self.get_balance_sheet()
            if not balance.empty:
                balance_millions = balance / 1_000_000
                report.append(balance_millions.round(1).to_string())
            else:
                report.append("No balance sheet data available")
            report.append("")
            
            # Cash Flow Statement
            report.append("CASH FLOW STATEMENT - Annual ($ Millions)")
            report.append("-" * 50)
            cashflow = self.get_cash_flow_statement()
            if not cashflow.empty:
                cashflow_millions = cashflow / 1_000_000
                report.append(cashflow_millions.round(1).to_string())
            else:
                report.append("No cash flow data available")
            report.append("")
            
            # Quarterly Trends
            report.append("QUARTERLY TRENDS (Last 2 Years)")
            report.append("-" * 50)
            quarterly = self.get_quarterly_data()
            if not quarterly.empty:
                quarterly_millions = quarterly / 1_000_000
                report.append(quarterly_millions.round(1).to_string())
            else:
                report.append("No quarterly data available")
            
        except Exception as e:
            report.append(f"Error generating report sections: {e}")
        
        return "\n".join(report)


# ─── Convenience Functions ─────────────────────────────────────────────────────
def analyze_company(ticker: str) -> FinancialAnalyzer:
    """Quick function to create analyzer for a company."""
    return FinancialAnalyzer(ticker)

def compare_companies(tickers: List[str], years: int = 3) -> pd.DataFrame:
    """Compare key metrics across multiple companies."""
    comparison_data = {}
    
    for ticker in tickers:
        try:
            analyzer = FinancialAnalyzer(ticker)
            metrics = analyzer.get_key_metrics(years=years)
            
            if not metrics.empty:
                # Get latest year data
                latest_year = metrics.index.max()
                comparison_data[ticker] = metrics.loc[latest_year]
                
        except Exception as e:
            print(f"Error analyzing {ticker}: {e}")
            continue
    
    if comparison_data:
        df = pd.DataFrame(comparison_data).T
        df.index.name = "Company"
        return df
    else:
        return pd.DataFrame()

# ─── Example Usage ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        print("=== MICROSOFT COMPREHENSIVE ANALYSIS ===")
        msft = analyze_company("MSFT")
        print(msft.generate_report())
        
        print("\n" + "="*100 + "\n")
        
        print("=== TECH GIANTS COMPARISON ===")
        comparison = compare_companies(["MSFT", "AAPL", "GOOGL"])
        if not comparison.empty:
            print("Latest Year Key Metrics Comparison:")
            print(comparison.round(2).to_string())
        
    except Exception as e:
        print(f"Error in example: {e}") 