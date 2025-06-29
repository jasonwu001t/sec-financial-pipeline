"""
Data Manager for handling Parquet file operations and data persistence.
Manages file storage, partitioning, and metadata tracking.
"""

import os
import json
import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor

from core.config import get_config
from core.models import (
    CompanyInfo, FinancialFact, ParquetFile, DataFreshness,
    ReportingPeriod, CompanyData
)


class DataManager:
    """Manages data storage and retrieval for SEC financial data."""
    
    def __init__(self):
        self.config = get_config()
        self.logger = logging.getLogger(__name__)
        self.base_path = Path(self.config.data_storage.company_facts_path)
        self.metadata_path = self.base_path / "metadata"
        
        # Ensure directories exist
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.metadata_path.mkdir(parents=True, exist_ok=True)
        
        # Track file metadata
        self._parquet_files: Dict[str, List[ParquetFile]] = {}
        self._data_freshness: Dict[str, DataFreshness] = {}
        
        # Load existing metadata
        self._load_metadata()
    
    def _load_metadata(self):
        """Load existing metadata from disk."""
        try:
            # Load parquet file metadata
            parquet_metadata_file = self.metadata_path / "parquet_files.json"
            if parquet_metadata_file.exists():
                with open(parquet_metadata_file, 'r') as f:
                    data = json.load(f)
                    for ticker, files_data in data.items():
                        self._parquet_files[ticker] = [
                            ParquetFile(**file_data) for file_data in files_data
                        ]
            
            # Load data freshness metadata
            freshness_metadata_file = self.metadata_path / "data_freshness.json"
            if freshness_metadata_file.exists():
                with open(freshness_metadata_file, 'r') as f:
                    data = json.load(f)
                    for ticker, freshness_data in data.items():
                        # Convert string dates back to datetime objects
                        if 'last_updated' in freshness_data:
                            freshness_data['last_updated'] = datetime.fromisoformat(
                                freshness_data['last_updated']
                            )
                        if 'last_sec_filing_date' in freshness_data and freshness_data['last_sec_filing_date']:
                            freshness_data['last_sec_filing_date'] = datetime.fromisoformat(
                                freshness_data['last_sec_filing_date']
                            ).date()
                        
                        self._data_freshness[ticker] = DataFreshness(**freshness_data)
                        
        except Exception as e:
            self.logger.warning(f"Failed to load metadata: {e}")
    
    def _save_metadata(self):
        """Save metadata to disk."""
        try:
            # Save parquet file metadata
            parquet_data = {}
            for ticker, files in self._parquet_files.items():
                parquet_data[ticker] = [file.dict() for file in files]
            
            parquet_metadata_file = self.metadata_path / "parquet_files.json"
            with open(parquet_metadata_file, 'w') as f:
                json.dump(parquet_data, f, indent=2, default=str)
            
            # Save data freshness metadata
            freshness_data = {}
            for ticker, freshness in self._data_freshness.items():
                freshness_data[ticker] = freshness.dict()
            
            freshness_metadata_file = self.metadata_path / "data_freshness.json"
            with open(freshness_metadata_file, 'w') as f:
                json.dump(freshness_data, f, indent=2, default=str)
                
        except Exception as e:
            self.logger.error(f"Failed to save metadata: {e}")
    
    def get_ticker_path(self, ticker: str) -> Path:
        """Get the directory path for a ticker."""
        return self.base_path / ticker.upper()
    
    def get_parquet_file_path(self, ticker: str, year: int, quarter: Optional[int] = None) -> Path:
        """Get the file path for a parquet file."""
        ticker_path = self.get_ticker_path(ticker)
        
        if quarter is not None:
            # Quarterly file
            period_path = ticker_path / "quarterly"
            filename = self.config.data_storage.file_naming["quarterly"].format(
                ticker=ticker.upper(), year=year, quarter=quarter
            )
        else:
            # Annual file
            period_path = ticker_path / "annual"
            filename = self.config.data_storage.file_naming["annual"].format(
                ticker=ticker.upper(), year=year
            )
        
        period_path.mkdir(parents=True, exist_ok=True)
        return period_path / filename
    
    def facts_to_dataframe(self, facts: List[FinancialFact]) -> pd.DataFrame:
        """Convert FinancialFact objects to pandas DataFrame."""
        if not facts:
            return pd.DataFrame()
        
        # Convert to list of dictionaries
        data = []
        for fact in facts:
            fact_dict = fact.dict()
            
            # Convert date objects to strings for parquet compatibility
            for date_field in ['start_date', 'end_date', 'instant_date']:
                if fact_dict.get(date_field):
                    fact_dict[date_field] = fact_dict[date_field].isoformat()
            
            # Convert enums to strings
            if fact_dict.get('unit'):
                fact_dict['unit'] = fact_dict['unit'].value if hasattr(fact_dict['unit'], 'value') else str(fact_dict['unit'])
            if fact_dict.get('form'):
                fact_dict['form'] = fact_dict['form'].value if hasattr(fact_dict['form'], 'value') else str(fact_dict['form'])
            
            data.append(fact_dict)
        
        df = pd.DataFrame(data)
        
        # Ensure consistent column types
        if 'fiscal_year' in df.columns:
            df['fiscal_year'] = pd.to_numeric(df['fiscal_year'], errors='coerce')
        if 'value' in df.columns:
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        return df
    
    def dataframe_to_facts(self, df: pd.DataFrame) -> List[FinancialFact]:
        """Convert pandas DataFrame back to FinancialFact objects."""
        facts = []
        
        for _, row in df.iterrows():
            try:
                # Convert string dates back to date objects
                row_dict = row.to_dict()
                
                for date_field in ['start_date', 'end_date', 'instant_date']:
                    if row_dict.get(date_field) and pd.notna(row_dict[date_field]):
                        try:
                            row_dict[date_field] = datetime.fromisoformat(row_dict[date_field]).date()
                        except (ValueError, TypeError):
                            row_dict[date_field] = None
                    else:
                        row_dict[date_field] = None
                
                # Handle NaN values
                for key, value in row_dict.items():
                    if pd.isna(value):
                        row_dict[key] = None
                
                fact = FinancialFact(**row_dict)
                facts.append(fact)
                
            except Exception as e:
                self.logger.warning(f"Failed to convert row to FinancialFact: {e}")
                continue
        
        return facts
    
    def save_company_data(self, company_info: CompanyInfo, facts: List[FinancialFact]) -> List[ParquetFile]:
        """Save company data to parquet files, partitioned by year and quarter."""
        ticker = company_info.ticker
        
        if not facts:
            self.logger.warning(f"No facts to save for {ticker}")
            return []
        
        # Convert to DataFrame for easier manipulation
        df = self.facts_to_dataframe(facts)
        
        if df.empty:
            self.logger.warning(f"Empty DataFrame for {ticker}")
            return []
        
        # Group by fiscal year and period
        created_files = []
        
        # Group by fiscal year
        if 'fiscal_year' in df.columns:
            for fiscal_year, year_df in df.groupby('fiscal_year'):
                if pd.isna(fiscal_year):
                    continue
                
                fiscal_year = int(fiscal_year)
                
                # Further split by fiscal period for quarterly data
                if 'fiscal_period' in year_df.columns:
                    annual_df = year_df[year_df['fiscal_period'].isna() | (year_df['fiscal_period'] == 'FY')]
                    quarterly_df = year_df[year_df['fiscal_period'].notna() & (year_df['fiscal_period'] != 'FY')]
                    
                    # Save annual data
                    if not annual_df.empty:
                        file_path = self.get_parquet_file_path(ticker, fiscal_year)
                        self._save_dataframe_to_parquet(annual_df, file_path)
                        
                        parquet_file = ParquetFile(
                            file_path=str(file_path),
                            ticker=ticker,
                            year=fiscal_year,
                            quarter=None,
                            statement_type="annual",
                            file_size_bytes=file_path.stat().st_size if file_path.exists() else None,
                            record_count=len(annual_df)
                        )
                        created_files.append(parquet_file)
                    
                    # Save quarterly data
                    for fiscal_period, quarter_df in quarterly_df.groupby('fiscal_period'):
                        if fiscal_period and fiscal_period.startswith('Q'):
                            try:
                                quarter = int(fiscal_period[1:])  # Extract quarter number from 'Q1', 'Q2', etc.
                                file_path = self.get_parquet_file_path(ticker, fiscal_year, quarter)
                                self._save_dataframe_to_parquet(quarter_df, file_path)
                                
                                parquet_file = ParquetFile(
                                    file_path=str(file_path),
                                    ticker=ticker,
                                    year=fiscal_year,
                                    quarter=quarter,
                                    statement_type="quarterly",
                                    file_size_bytes=file_path.stat().st_size if file_path.exists() else None,
                                    record_count=len(quarter_df)
                                )
                                created_files.append(parquet_file)
                                
                            except (ValueError, IndexError):
                                self.logger.warning(f"Could not parse quarter from fiscal_period: {fiscal_period}")
                                continue
                else:
                    # No fiscal period info, save as annual
                    file_path = self.get_parquet_file_path(ticker, fiscal_year)
                    self._save_dataframe_to_parquet(year_df, file_path)
                    
                    parquet_file = ParquetFile(
                        file_path=str(file_path),
                        ticker=ticker,
                        year=fiscal_year,
                        quarter=None,
                        statement_type="annual",
                        file_size_bytes=file_path.stat().st_size if file_path.exists() else None,
                        record_count=len(year_df)
                    )
                    created_files.append(parquet_file)
        
        # Update metadata
        self._parquet_files[ticker] = created_files
        self._update_data_freshness(ticker, facts)
        self._save_metadata()
        
        self.logger.info(f"Saved {len(created_files)} parquet files for {ticker}")
        return created_files
    
    def _save_dataframe_to_parquet(self, df: pd.DataFrame, file_path: Path):
        """Save DataFrame to parquet file with compression."""
        try:
            # Ensure directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save with compression
            df.to_parquet(
                file_path,
                compression=self.config.data_storage.parquet_compression,
                index=False
            )
            
            self.logger.debug(f"Saved {len(df)} records to {file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save parquet file {file_path}: {e}")
            raise
    
    def _update_data_freshness(self, ticker: str, facts: List[FinancialFact]):
        """Update data freshness metadata for a ticker."""
        # Find the latest filing date
        latest_filing_date = None
        annual_years = set()
        quarterly_periods = set()
        
        for fact in facts:
            # Use end_date or instant_date as filing reference
            filing_date = fact.end_date or fact.instant_date
            if filing_date and (latest_filing_date is None or filing_date > latest_filing_date):
                latest_filing_date = filing_date
            
            # Track coverage
            if fact.fiscal_year:
                if fact.fiscal_period in [None, 'FY']:
                    annual_years.add(fact.fiscal_year)
                elif fact.fiscal_period and fact.fiscal_period.startswith('Q'):
                    quarterly_periods.add(f"{fact.fiscal_year}-{fact.fiscal_period}")
        
        self._data_freshness[ticker] = DataFreshness(
            ticker=ticker,
            last_updated=datetime.utcnow(),
            last_sec_filing_date=latest_filing_date,
            annual_data_years=sorted(list(annual_years)),
            quarterly_data_periods=sorted(list(quarterly_periods)),
            needs_update=False
        )
    
    def load_company_data(self, ticker: str, years: Optional[int] = None) -> Optional[CompanyData]:
        """Load company data from parquet files."""
        ticker = ticker.upper()
        
        if ticker not in self._parquet_files:
            return None
        
        all_facts = []
        parquet_files = self._parquet_files[ticker]
        
        # Filter by years if specified
        if years:
            current_year = datetime.now().year
            min_year = current_year - years + 1
            parquet_files = [f for f in parquet_files if f.year >= min_year]
        
        # Load data from parquet files
        for parquet_file in parquet_files:
            try:
                file_path = Path(parquet_file.file_path)
                if file_path.exists():
                    df = pd.read_parquet(file_path)
                    facts = self.dataframe_to_facts(df)
                    all_facts.extend(facts)
                    
            except Exception as e:
                self.logger.error(f"Failed to load parquet file {parquet_file.file_path}: {e}")
                continue
        
        if not all_facts:
            return None
        
        # Create company info from the first fact or metadata
        company_info = CompanyInfo(
            ticker=ticker,
            cik="",  # Will be filled from facts or external source
            name=f"Company {ticker}"  # Will be filled from external source
        )
        
        # Create company data object
        company_data = CompanyData(
            company_info=company_info,
            last_updated=datetime.utcnow(),
            raw_facts=all_facts
        )
        
        return company_data
    
    def get_data_freshness(self, ticker: str) -> Optional[DataFreshness]:
        """Get data freshness information for a ticker."""
        return self._data_freshness.get(ticker.upper())
    
    def list_available_tickers(self) -> List[str]:
        """List all tickers with available data."""
        return list(self._parquet_files.keys())
    
    def get_ticker_file_info(self, ticker: str) -> List[ParquetFile]:
        """Get file information for a ticker."""
        return self._parquet_files.get(ticker.upper(), [])
    
    def delete_ticker_data(self, ticker: str) -> bool:
        """Delete all data for a ticker."""
        ticker = ticker.upper()
        
        try:
            # Delete parquet files
            if ticker in self._parquet_files:
                for parquet_file in self._parquet_files[ticker]:
                    file_path = Path(parquet_file.file_path)
                    if file_path.exists():
                        file_path.unlink()
                
                # Remove from metadata
                del self._parquet_files[ticker]
            
            if ticker in self._data_freshness:
                del self._data_freshness[ticker]
            
            # Delete directory if empty
            ticker_path = self.get_ticker_path(ticker)
            if ticker_path.exists():
                try:
                    ticker_path.rmdir()
                except OSError:
                    pass  # Directory not empty
            
            self._save_metadata()
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete data for {ticker}: {e}")
            return False
    
    def calculate_storage_stats(self) -> Dict[str, Any]:
        """Calculate storage statistics."""
        total_files = 0
        total_size = 0
        total_records = 0
        
        for ticker, files in self._parquet_files.items():
            total_files += len(files)
            for file in files:
                if file.file_size_bytes:
                    total_size += file.file_size_bytes
                if file.record_count:
                    total_records += file.record_count
        
        return {
            "total_tickers": len(self._parquet_files),
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "total_records": total_records,
            "avg_records_per_ticker": round(total_records / len(self._parquet_files), 0) if self._parquet_files else 0
        }


# Helper functions

def create_data_manager() -> DataManager:
    """Create a new DataManager instance."""
    return DataManager()


if __name__ == "__main__":
    # Test the data manager
    manager = DataManager()
    stats = manager.calculate_storage_stats()
    print(f"Storage stats: {stats}")
    
    available_tickers = manager.list_available_tickers()
    print(f"Available tickers: {available_tickers[:10]}...")  # Show first 10 