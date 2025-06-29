"""
Quick-start SEC financials helper
─────────────────────────────────
• No API-key needed – uses SEC's public JSON endpoints
• Ticker-to-CIK mapping downloaded once then cached in memory
• Supports any XBRL tag published by the SEC Company Facts API
• Simple metric aliases (EPS, revenue, expenses…) included out-of-the-box
"""

from __future__ import annotations
import datetime as _dt
import json
from functools import lru_cache
from typing import Iterable, Sequence

import pandas as pd
import requests

# ─── Constants ──────────────────────────────────────────────────────────────────
_USER_AGENT = (
    "sec-financial-pipeline/1.0 "
    "(Contact: jasonwu001t@gmail.com) "
    "- GitHub: jasonwu001t/sec-financial-pipeline"
)
_TICKERS_JSON = "https://www.sec.gov/files/company_tickers.json"
_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# ─── Comprehensive Financial Statement Mappings ────────────────────────────────
# Each friendly name maps to a list of possible XBRL tags (in order of preference)

# Income Statement Items
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

# Balance Sheet Items
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

# Cash Flow Statement Items
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

# Other Important Metrics
OTHER_METRICS = {
    "dividends_per_share": ["CommonStockDividendsPerShareDeclared"],
    "book_value_per_share": ["BookValuePerShare"],
}

# Legacy simple mapping for backward compatibility
_ALIAS_TO_TAG = {
    "eps_basic": "EarningsPerShareBasic",
    "eps_diluted": "EarningsPerShareDiluted", 
    "eps": "EarningsPerShareDiluted",
    "revenue": "RevenueFromContractWithCustomerExcludingAssessedTax",
    "sales": "RevenueFromContractWithCustomerExcludingAssessedTax",
    "expenses": "OperatingExpenses",
    "net_income": "NetIncomeLoss",
    "cogs": "CostOfGoodsAndServicesSold",
    "cash": "CashAndCashEquivalentsAtCarryingValue",
    "assets": "Assets",
    "liabilities": "Liabilities",
}

# ─── Internal helpers ──────────────────────────────────────────────────────────
@lru_cache(maxsize=1)
def _ticker_to_cik() -> dict[str, str]:
    """Download SEC's master ticker list once and cache it."""
    resp = requests.get(_TICKERS_JSON, headers={"User-Agent": _USER_AGENT}, timeout=30)
    resp.raise_for_status()
    raw = resp.json()
    
    # The SEC API returns a dict where keys are string indices and values contain ticker info
    return {row["ticker"].upper(): str(row["cik_str"]).zfill(10) for row in raw.values()}


def _normalize_metric(metric: str) -> str:
    """Map friendly alias → official tag (case-insensitive)."""
    metric = metric.lower()
    return _ALIAS_TO_TAG.get(metric, metric)


def _find_best_revenue_tag(facts: dict) -> str:
    """Find the best revenue tag available in the company's facts."""
    # Try revenue tags in order of preference
    revenue_tags = [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues", 
        "SalesRevenueNet",
        "SalesRevenueGoodsNet"
    ]
    
    for tag in revenue_tags:
        if tag in facts:
            return tag
    
    # If none found, return the default
    return "RevenueFromContractWithCustomerExcludingAssessedTax"


def _find_best_tag(tag_options: list[str], facts: dict) -> str:
    """Find the first available tag from a list of options."""
    for tag in tag_options:
        if tag in facts:
            return tag
    return tag_options[0]  # Return first option as fallback


def _extract_comprehensive_data(
    facts: dict,
    items_mapping: dict[str, list[str]], 
    period: str = "annual",
    start: int | None = None,
    end: int | None = None,
) -> dict[str, dict[int, float]]:
    """Extract comprehensive financial data for specified items."""
    data = {}
    
    for friendly_name, tag_options in items_mapping.items():
        best_tag = _find_best_tag(tag_options, facts)
        
        if best_tag not in facts:
            data[friendly_name] = {}
            continue
            
        entries = _pick_preferred_unit(facts[best_tag]["units"])
        yearly_data = {}
        
        for item in entries:
            fy = item.get("fy")
            fp = item.get("fp", "")
            
            if fy is None:
                continue
                
            # Period filter
            if period == "annual" and not fp.startswith("FY"):
                continue
            elif period == "quarterly" and fp.startswith("FY"):
                continue
            
            # Date range filter  
            if start and fy < start:
                continue
            if end and fy > end:
                continue
                
            # Create appropriate key
            if period == "quarterly":
                key = f"{fy}-{fp}"
                yearly_data[key] = item["val"]
            else:
                yearly_data[fy] = item["val"]
                
        data[friendly_name] = yearly_data
    
    return data


def _pick_preferred_unit(unit_dict: dict[str, list[dict]]) -> list[dict]:
    """
    CompanyFacts returns a separate array for every measurement unit
    (USD, shares, etc.).  We naïvely pick the first unit, but you can add
    smarter logic here if needed.
    """
    return next(iter(unit_dict.values()))


# ─── Public function ───────────────────────────────────────────────────────────
def get_financials(
    company: str,
    metrics: Sequence[str] | str,
    *,
    period: str = "annual",         # "annual" | "quarterly" | "all"
    start: int | None = None,       # fiscal year filter
    end: int | None = None,
) -> pd.DataFrame:
    """
    Parameters
    ----------
    company : str
        Ticker symbol («AAPL», «TSLA», …) **or** 10-digit CIK.
    metrics : str | list[str]
        One or many aliases or raw XBRL tags (see _ALIAS_TO_TAG).
    period : str
        "annual" (default) keeps only FY data,
        "quarterly" keeps Q1…Q4, "all" keeps everything.
    start, end : int | None
        Filter by fiscal year (inclusive).  If omitted, returns full history.

    Returns
    -------
    pd.DataFrame with columns = metric names, indexed by fiscal year.
    """
    # Resolve CIK
    cik = company
    if not company.isdigit():
        mapping = _ticker_to_cik()
        cik = mapping.get(company.upper())
        if cik is None:
            raise ValueError(f"Unknown ticker {company!r}")
    
    print(f"Using CIK: {cik} for company: {company}")

    # Download CompanyFacts JSON once
    url = _FACTS_URL.format(cik=cik)
    print(f"Fetching facts from: {url}")
    resp = requests.get(url, headers={"User-Agent": _USER_AGENT}, timeout=30)
    print(f"Response status: {resp.status_code}")
    
    if resp.status_code != 200:
        print(f"Error: {resp.status_code} - {resp.text[:500]}")
        raise ValueError(f"SEC API returned {resp.status_code} for CIK {cik}")
    
    facts = resp.json()
    facts = facts["facts"]["us-gaap"]

    # Normalise metrics list
    if isinstance(metrics, str):
        metrics = [metrics]
    tags = {m: _normalize_metric(m) for m in metrics}
    
    # Use the best available revenue tag for revenue/sales metrics
    for metric, tag in tags.items():
        if metric.lower() in ['revenue', 'sales'] and tag not in facts:
            tags[metric] = _find_best_revenue_tag(facts)

    # Build {metric: {year: value}} dict
    data: dict[str, dict[int, float]] = {m: {} for m in tags.keys()}

    for friendly, tag in tags.items():
        if tag not in facts:
            continue  # metric not reported by this issuer
        entries = _pick_preferred_unit(facts[tag]["units"])
        for item in entries:
            fy = item.get("fy")               # fiscal year
            fp = item.get("fp", "")           # FY, Q1, etc.
            if fy is None:
                continue
            # Period filter
            if period == "annual" and not fp.startswith("FY"):
                continue
            if period == "quarterly" and fp.startswith("FY"):
                continue
            if start and fy < start:
                continue
            if end and fy > end:
                continue
            data[friendly][fy] = item["val"]

    # Convert to tidy DataFrame
    df = pd.DataFrame(data).sort_index()
    df.index.name = "Fiscal Year"
    return df


# ─── Comprehensive Financial Analysis Functions ────────────────────────────────
def get_income_statement(
    company: str,
    *,
    period: str = "annual",
    years: int = 5,
) -> pd.DataFrame:
    """
    Get comprehensive income statement.
    
    Parameters
    ----------
    company : str
        Ticker symbol or CIK
    period : str
        "annual" (default) or "quarterly" 
    years : int
        Number of years of data to retrieve (default 5)
        
    Returns
    -------
    pd.DataFrame with income statement items as rows, years/periods as columns
    """
    # Resolve CIK and fetch facts
    cik = company
    if not company.isdigit():
        mapping = _ticker_to_cik()
        cik = mapping.get(company.upper())
        if cik is None:
            raise ValueError(f"Unknown ticker {company!r}")
    
    url = _FACTS_URL.format(cik=cik)
    resp = requests.get(url, headers={"User-Agent": _USER_AGENT}, timeout=30)
    if resp.status_code != 200:
        raise ValueError(f"SEC API returned {resp.status_code} for CIK {cik}")
    
    facts = resp.json()["facts"]["us-gaap"]
    
    # Extract data
    end_year = _dt.datetime.now().year
    start_year = end_year - years + 1
    
    data = _extract_comprehensive_data(facts, INCOME_STATEMENT_ITEMS, period, start_year, end_year)
    
    df = pd.DataFrame(data).T
    df = df.sort_index(axis=1)
    df.index.name = "Income Statement Item"
    
    return df


def get_balance_sheet(
    company: str,
    *,
    period: str = "annual", 
    years: int = 5,
) -> pd.DataFrame:
    """
    Get comprehensive balance sheet.
    
    Parameters
    ----------
    company : str
        Ticker symbol or CIK
    period : str
        "annual" (default) or "quarterly"
    years : int
        Number of years of data to retrieve (default 5)
        
    Returns
    -------
    pd.DataFrame with balance sheet items as rows, years/periods as columns
    """
    # Resolve CIK and fetch facts
    cik = company
    if not company.isdigit():
        mapping = _ticker_to_cik()
        cik = mapping.get(company.upper())
        if cik is None:
            raise ValueError(f"Unknown ticker {company!r}")
    
    url = _FACTS_URL.format(cik=cik)
    resp = requests.get(url, headers={"User-Agent": _USER_AGENT}, timeout=30)
    if resp.status_code != 200:
        raise ValueError(f"SEC API returned {resp.status_code} for CIK {cik}")
    
    facts = resp.json()["facts"]["us-gaap"]
    
    # Extract data
    end_year = _dt.datetime.now().year
    start_year = end_year - years + 1
    
    data = _extract_comprehensive_data(facts, BALANCE_SHEET_ITEMS, period, start_year, end_year)
    
    df = pd.DataFrame(data).T
    df = df.sort_index(axis=1)
    df.index.name = "Balance Sheet Item"
    
    return df


def get_cash_flow_statement(
    company: str,
    *,
    period: str = "annual",
    years: int = 5,
) -> pd.DataFrame:
    """
    Get comprehensive cash flow statement.
    
    Parameters
    ----------
    company : str
        Ticker symbol or CIK
    period : str
        "annual" (default) or "quarterly"
    years : int
        Number of years of data to retrieve (default 5)
        
    Returns
    -------
    pd.DataFrame with cash flow items as rows, years/periods as columns
    """
    # Resolve CIK and fetch facts
    cik = company
    if not company.isdigit():
        mapping = _ticker_to_cik()
        cik = mapping.get(company.upper())
        if cik is None:
            raise ValueError(f"Unknown ticker {company!r}")
    
    url = _FACTS_URL.format(cik=cik)
    resp = requests.get(url, headers={"User-Agent": _USER_AGENT}, timeout=30)
    if resp.status_code != 200:
        raise ValueError(f"SEC API returned {resp.status_code} for CIK {cik}")
    
    facts = resp.json()["facts"]["us-gaap"]
    
    # Extract data
    end_year = _dt.datetime.now().year
    start_year = end_year - years + 1
    
    data = _extract_comprehensive_data(facts, CASH_FLOW_ITEMS, period, start_year, end_year)
    
    df = pd.DataFrame(data).T
    df = df.sort_index(axis=1)
    df.index.name = "Cash Flow Item"
    
    return df


def calculate_financial_ratios(
    company: str,
    years: int = 5,
) -> pd.DataFrame:
    """
    Calculate key financial ratios and metrics.
    
    Parameters
    ----------
    company : str
        Ticker symbol or CIK
    years : int
        Number of years of data for calculations (default 5)
        
    Returns
    -------
    pd.DataFrame with financial ratios and metrics
    """
    # Get the underlying financial data
    income_df = get_income_statement(company, years=years)
    balance_df = get_balance_sheet(company, years=years)
    
    ratios_data = {}
    
    # Get available years (intersection of income and balance sheet data)
    available_years = []
    if not income_df.empty and not balance_df.empty:
        available_years = sorted(set(income_df.columns) & set(balance_df.columns))
    
    for year in available_years:
        if not isinstance(year, int):
            continue
            
        year_ratios = {}
        
        # Get base financial values
        revenue = income_df.loc["revenue", year] if "revenue" in income_df.index else 0
        gross_profit = income_df.loc["gross_profit", year] if "gross_profit" in income_df.index else 0
        operating_income = income_df.loc["operating_income", year] if "operating_income" in income_df.index else 0
        net_income = income_df.loc["net_income", year] if "net_income" in income_df.index else 0
        
        total_assets = balance_df.loc["total_assets", year] if "total_assets" in balance_df.index else 0
        total_equity = balance_df.loc["total_equity", year] if "total_equity" in balance_df.index else 0
        total_liabilities = balance_df.loc["total_liabilities", year] if "total_liabilities" in balance_df.index else 0
        
        # Calculate key metrics (in millions for readability)
        year_ratios["revenue_millions"] = revenue / 1_000_000 if revenue else 0
        year_ratios["net_income_millions"] = net_income / 1_000_000 if net_income else 0
        year_ratios["total_assets_millions"] = total_assets / 1_000_000 if total_assets else 0
        
        # Calculate margin ratios (as percentages)
        if revenue > 0:
            year_ratios["gross_margin_%"] = (gross_profit / revenue) * 100
            year_ratios["operating_margin_%"] = (operating_income / revenue) * 100 
            year_ratios["net_margin_%"] = (net_income / revenue) * 100
        
        # Calculate returns (as percentages)
        if total_assets > 0:
            year_ratios["roa_%"] = (net_income / total_assets) * 100
        
        if total_equity > 0:
            year_ratios["roe_%"] = (net_income / total_equity) * 100
            year_ratios["debt_to_equity"] = total_liabilities / total_equity
        
        # Get EPS data
        year_ratios["eps_basic"] = income_df.loc["eps_basic", year] if "eps_basic" in income_df.index else 0
        year_ratios["eps_diluted"] = income_df.loc["eps_diluted", year] if "eps_diluted" in income_df.index else 0
        
        ratios_data[year] = year_ratios
    
    if ratios_data:
        df = pd.DataFrame(ratios_data).T
        df.index.name = "Year"
        return df
    else:
        return pd.DataFrame()


def get_quarterly_data(
    company: str,
    metrics: list[str] = None,
    years: int = 2,
) -> pd.DataFrame:
    """
    Get quarterly data for key metrics.
    
    Parameters
    ----------
    company : str
        Ticker symbol or CIK
    metrics : list[str], optional
        List of metrics to retrieve. If None, uses default set.
    years : int
        Number of years of quarterly data (default 2)
        
    Returns
    -------
    pd.DataFrame with quarterly data
    """
    if metrics is None:
        metrics = ["revenue", "net_income", "eps_diluted", "operating_income"]
    
    # Build metrics mapping
    metrics_mapping = {}
    for metric in metrics:
        if metric in INCOME_STATEMENT_ITEMS:
            metrics_mapping[metric] = INCOME_STATEMENT_ITEMS[metric]
        elif metric in BALANCE_SHEET_ITEMS:
            metrics_mapping[metric] = BALANCE_SHEET_ITEMS[metric]
        elif metric in CASH_FLOW_ITEMS:
            metrics_mapping[metric] = CASH_FLOW_ITEMS[metric]
    
    # Resolve CIK and fetch facts
    cik = company
    if not company.isdigit():
        mapping = _ticker_to_cik()  
        cik = mapping.get(company.upper())
        if cik is None:
            raise ValueError(f"Unknown ticker {company!r}")
    
    url = _FACTS_URL.format(cik=cik)
    resp = requests.get(url, headers={"User-Agent": _USER_AGENT}, timeout=30)
    if resp.status_code != 200:
        raise ValueError(f"SEC API returned {resp.status_code} for CIK {cik}")
    
    facts = resp.json()["facts"]["us-gaap"]
    
    # Extract quarterly data
    end_year = _dt.datetime.now().year
    start_year = end_year - years + 1
    
    data = _extract_comprehensive_data(facts, metrics_mapping, "quarterly", start_year, end_year)
    
    df = pd.DataFrame(data).T
    df = df.sort_index(axis=1)
    df.index.name = "Quarterly Metric"
    
    return df


def generate_comprehensive_report(company: str) -> str:
    """
    Generate a comprehensive financial analysis report.
    
    Parameters
    ----------
    company : str
        Ticker symbol or CIK
        
    Returns
    -------
    str
        Formatted comprehensive financial report
    """
    report_lines = []
    report_lines.append(f"COMPREHENSIVE FINANCIAL ANALYSIS: {company.upper()}")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    try:
        # Key Financial Ratios
        report_lines.append("KEY FINANCIAL METRICS & RATIOS (Last 5 Years)")
        report_lines.append("-" * 60)
        ratios = calculate_financial_ratios(company)
        if not ratios.empty:
            report_lines.append(ratios.round(2).to_string())
        else:
            report_lines.append("No ratio data available")
        report_lines.append("")
        
        # Income Statement
        report_lines.append("INCOME STATEMENT - Annual ($ Millions)")
        report_lines.append("-" * 60) 
        income_stmt = get_income_statement(company)
        if not income_stmt.empty:
            income_millions = income_stmt / 1_000_000
            report_lines.append(income_millions.round(1).to_string())
        else:
            report_lines.append("No income statement data available")
        report_lines.append("")
        
        # Balance Sheet
        report_lines.append("BALANCE SHEET - Annual ($ Millions)")
        report_lines.append("-" * 60)
        balance_sheet = get_balance_sheet(company)
        if not balance_sheet.empty:
            balance_millions = balance_sheet / 1_000_000
            report_lines.append(balance_millions.round(1).to_string())
        else:
            report_lines.append("No balance sheet data available")
        report_lines.append("")
        
        # Cash Flow Statement
        report_lines.append("CASH FLOW STATEMENT - Annual ($ Millions)")
        report_lines.append("-" * 60)
        cash_flow = get_cash_flow_statement(company)
        if not cash_flow.empty:
            cash_flow_millions = cash_flow / 1_000_000
            report_lines.append(cash_flow_millions.round(1).to_string())
        else:
            report_lines.append("No cash flow data available")
        report_lines.append("")
        
        # Quarterly Trends
        report_lines.append("QUARTERLY TRENDS - Key Metrics (Last 2 Years, $ Millions)")
        report_lines.append("-" * 60)
        quarterly = get_quarterly_data(company)
        if not quarterly.empty:
            quarterly_millions = quarterly / 1_000_000
            report_lines.append(quarterly_millions.round(1).to_string())
        else:
            report_lines.append("No quarterly data available")
        
    except Exception as e:
        report_lines.append(f"Error generating comprehensive report: {e}")
    
    return "\n".join(report_lines)


def compare_companies(
    companies: list[str],
    metric: str = "revenue",
    years: int = 3,
) -> pd.DataFrame:
    """
    Compare a specific metric across multiple companies.
    
    Parameters
    ----------
    companies : list[str]
        List of ticker symbols or CIKs
    metric : str
        Financial metric to compare (default "revenue")
    years : int
        Number of years to compare (default 3)
        
    Returns
    -------
    pd.DataFrame with companies as columns, years as rows
    """
    comparison_data = {}
    
    for company in companies:
        try:
            if metric in INCOME_STATEMENT_ITEMS:
                df = get_income_statement(company, years=years)
            elif metric in BALANCE_SHEET_ITEMS:
                df = get_balance_sheet(company, years=years)
            elif metric in CASH_FLOW_ITEMS:
                df = get_cash_flow_statement(company, years=years)
            else:
                print(f"Unknown metric: {metric}")
                continue
                
            if not df.empty and metric in df.index:
                comparison_data[company.upper()] = df.loc[metric]
                
        except Exception as e:
            print(f"Error analyzing {company}: {e}")
            continue
    
    if comparison_data:
        df = pd.DataFrame(comparison_data)
        df = df.sort_index()
        df.index.name = "Year"
        return df
    else:
        return pd.DataFrame()


# ─── Example usage ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🏦 COMPREHENSIVE SEC FINANCIAL ANALYZER")
    print("=" * 50)
    
    # Example 1: Simple Legacy Function (still works)
    print("\n📊 Example 1: Simple Revenue & EPS Query")
    print("-" * 40)
    try:
        simple_data = get_financials("MSFT", metrics=["revenue", "eps"], start=2022)
        print("Microsoft Revenue & EPS (2022+):")
        print(simple_data)
    except Exception as e:
        print(f"Error: {e}")
    
    # Example 2: Complete Financial Statements
    print("\n📈 Example 2: Complete Income Statement")
    print("-" * 40)
    try:
        income_stmt = get_income_statement("MSFT", years=3)
        print("Microsoft Income Statement (Last 3 Years, $ Millions):")
        print((income_stmt / 1_000_000).round(1))
    except Exception as e:
        print(f"Error: {e}")
    
    # Example 3: Financial Ratios & Key Metrics
    print("\n💰 Example 3: Financial Ratios & Key Metrics")
    print("-" * 40)
    try:
        ratios = calculate_financial_ratios("MSFT", years=3)
        print("Microsoft Key Financial Metrics:")
        print(ratios.round(2))
    except Exception as e:
        print(f"Error: {e}")
    
    # Example 4: Quarterly Trends
    print("\n📅 Example 4: Quarterly Data")
    print("-" * 40)
    try:
        quarterly = get_quarterly_data("MSFT", metrics=["revenue", "net_income"], years=2)
        print("Microsoft Quarterly Trends ($ Millions):")
        print((quarterly / 1_000_000).round(1))
    except Exception as e:
        print(f"Error: {e}")
    
    # Example 5: Company Comparison
    print("\n🔍 Example 5: Tech Giants Revenue Comparison")
    print("-" * 40)
    try:
        comparison = compare_companies(["MSFT", "AAPL", "GOOGL"], metric="revenue", years=3)
        print("Revenue Comparison ($ Billions):")
        print((comparison / 1_000_000_000).round(1))
    except Exception as e:
        print(f"Error: {e}")
    
    # Example 6: Comprehensive Report
    print("\n📋 Example 6: Full Comprehensive Report")
    print("-" * 40)
    print("Generating comprehensive report for Apple...")
    try:
        report = generate_comprehensive_report("AAPL")
        # Print first part of report to avoid overwhelming output
        report_lines = report.split('\n')
        print('\n'.join(report_lines[:50]))  # First 50 lines
        if len(report_lines) > 50:
            print(f"\n... [Report continues for {len(report_lines) - 50} more lines] ...")
            print("\nUse generate_comprehensive_report('AAPL') to see the full report!")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n✅ Financial analyzer ready!")
    print("\nAvailable functions:")
    print("• get_income_statement(company, period='annual', years=5)")
    print("• get_balance_sheet(company, period='annual', years=5)")  
    print("• get_cash_flow_statement(company, period='annual', years=5)")
    print("• calculate_financial_ratios(company, years=5)")
    print("• get_quarterly_data(company, metrics=None, years=2)")
    print("• generate_comprehensive_report(company)")
    print("• compare_companies(companies, metric='revenue', years=3)")
    print("\nFor quarterly data, use period='quarterly'")
    print("For custom metrics, see INCOME_STATEMENT_ITEMS, BALANCE_SHEET_ITEMS, CASH_FLOW_ITEMS")
