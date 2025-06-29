"""
Data models for SEC Financial Data Pipeline.
Defines Pydantic models for financial data structures and API responses.
"""

from typing import Dict, List, Any, Optional, Union, Literal
from datetime import datetime, date
from decimal import Decimal
from pydantic import BaseModel, Field, validator, root_validator
from enum import Enum


class ReportingPeriod(str, Enum):
    """Enumeration of reporting periods."""
    ANNUAL = "annual"
    QUARTERLY = "quarterly"
    
    
class FormType(str, Enum):
    """SEC form types."""
    FORM_10K = "10-K"
    FORM_10Q = "10-Q"
    FORM_8K = "8-K"
    

class UnitType(str, Enum):
    """XBRL unit types."""
    USD = "USD"
    SHARES = "shares"
    PURE = "pure"  # For ratios, percentages
    

class FinancialFact(BaseModel):
    """Individual financial fact from SEC XBRL data."""
    label: str
    description: Optional[str] = None
    value: Optional[Union[float, int]] = None
    unit: Optional[UnitType] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    instant_date: Optional[date] = None
    form: Optional[FormType] = None
    fiscal_year: Optional[int] = None
    fiscal_period: Optional[str] = None
    frame: Optional[str] = None
    
    class Config:
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
            datetime: lambda v: v.isoformat() if v else None,
            Decimal: lambda v: float(v) if v else None
        }


class CompanyInfo(BaseModel):
    """Company identification and basic information."""
    cik: str = Field(..., description="Central Index Key")
    ticker: str = Field(..., description="Stock ticker symbol")
    name: str = Field(..., description="Company name")
    industry: Optional[str] = None
    sector: Optional[str] = None
    exchange: Optional[str] = None
    
    @validator('cik')
    def validate_cik(cls, v):
        """Ensure CIK is 10 digits with leading zeros."""
        if isinstance(v, str):
            return v.zfill(10)
        return str(v).zfill(10)
    
    @validator('ticker')
    def validate_ticker(cls, v):
        """Ensure ticker is uppercase."""
        return v.upper() if v else None


class FinancialStatement(BaseModel):
    """Financial statement data for a specific period."""
    company_info: CompanyInfo
    statement_type: str = Field(..., description="Type of financial statement")
    reporting_period: ReportingPeriod
    period_end_date: date
    fiscal_year: int
    fiscal_period: Optional[str] = None
    facts: List[FinancialFact] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
            datetime: lambda v: v.isoformat() if v else None
        }


class IncomeStatement(FinancialStatement):
    """Income statement specific model."""
    statement_type: Literal["income_statement"] = "income_statement"
    
    # Common income statement items
    revenue: Optional[float] = None
    gross_profit: Optional[float] = None
    operating_income: Optional[float] = None
    net_income: Optional[float] = None
    earnings_per_share: Optional[float] = None
    

class BalanceSheet(FinancialStatement):
    """Balance sheet specific model."""
    statement_type: Literal["balance_sheet"] = "balance_sheet"
    
    # Common balance sheet items
    total_assets: Optional[float] = None
    total_liabilities: Optional[float] = None
    shareholders_equity: Optional[float] = None
    cash_and_cash_equivalents: Optional[float] = None
    

class CashFlowStatement(FinancialStatement):
    """Cash flow statement specific model."""
    statement_type: Literal["cash_flow"] = "cash_flow"
    
    # Common cash flow items
    operating_cash_flow: Optional[float] = None
    investing_cash_flow: Optional[float] = None
    financing_cash_flow: Optional[float] = None
    net_cash_flow: Optional[float] = None


class CompanyData(BaseModel):
    """Complete company financial data."""
    company_info: CompanyInfo
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    data_source_url: Optional[str] = None
    
    # Financial statements by period
    income_statements: List[IncomeStatement] = Field(default_factory=list)
    balance_sheets: List[BalanceSheet] = Field(default_factory=list)
    cash_flow_statements: List[CashFlowStatement] = Field(default_factory=list)
    
    # Raw facts for flexibility
    raw_facts: List[FinancialFact] = Field(default_factory=list)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
            date: lambda v: v.isoformat() if v else None
        }


# API Response Models

class APIResponse(BaseModel):
    """Base API response model."""
    success: bool = True
    message: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class FinancialDataResponse(APIResponse):
    """Response model for financial data endpoints."""
    data: Union[CompanyData, List[FinancialStatement], List[FinancialFact]]
    ticker: Optional[str] = None
    metric: Optional[str] = None
    period: Optional[ReportingPeriod] = None
    years: Optional[int] = None


class MetricResponse(APIResponse):
    """Response model for specific metric queries."""
    data: List[Dict[str, Any]]
    ticker: str
    metric: str
    period: ReportingPeriod
    years: int


class HealthCheckResponse(APIResponse):
    """Response model for health check endpoint."""
    status: str
    api_version: str
    data_freshness: Dict[str, Any]
    cache_stats: Optional[Dict[str, Any]] = None


class ErrorResponse(APIResponse):
    """Response model for error cases."""
    success: bool = False
    error_type: str
    error_code: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


# ETL Models

class ETLJobStatus(str, Enum):
    """ETL job status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ETLJob(BaseModel):
    """ETL job model."""
    job_id: str
    ticker: str
    job_type: str = Field(description="Type of ETL job (full, incremental, on-demand)")
    status: ETLJobStatus = ETLJobStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    records_processed: int = 0
    files_created: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class DataFreshness(BaseModel):
    """Data freshness tracking model."""
    ticker: str
    last_updated: datetime
    last_sec_filing_date: Optional[date] = None
    annual_data_years: List[int] = Field(default_factory=list)
    quarterly_data_periods: List[str] = Field(default_factory=list)
    needs_update: bool = False
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
            date: lambda v: v.isoformat() if v else None
        }


# Query Models

class FinancialQuery(BaseModel):
    """Base model for financial data queries."""
    ticker: str = Field(..., description="Stock ticker symbol")
    period: ReportingPeriod = Field(default=ReportingPeriod.ANNUAL)
    years: int = Field(default=5, ge=1, le=20, description="Number of years of data")
    
    @validator('ticker')
    def validate_ticker(cls, v):
        return v.upper()


class MetricQuery(FinancialQuery):
    """Query model for specific financial metrics."""
    metric: str = Field(..., description="Financial metric name")
    format: str = Field(default="json", description="Response format (json, csv)")


class ComparisonQuery(BaseModel):
    """Query model for company comparisons."""
    tickers: List[str] = Field(..., min_items=2, max_items=10)
    metric: str = Field(..., description="Metric to compare")
    period: ReportingPeriod = Field(default=ReportingPeriod.ANNUAL)
    years: int = Field(default=3, ge=1, le=10)
    
    @validator('tickers')
    def validate_tickers(cls, v):
        return [ticker.upper() for ticker in v]


# Cache Models

class CacheEntry(BaseModel):
    """Cache entry model."""
    key: str
    data: Any
    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime
    size_bytes: Optional[int] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


# File Storage Models

class ParquetFile(BaseModel):
    """Parquet file metadata model."""
    file_path: str
    ticker: str
    year: int
    quarter: Optional[int] = None
    statement_type: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    file_size_bytes: Optional[int] = None
    record_count: Optional[int] = None
    checksum: Optional[str] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        } 