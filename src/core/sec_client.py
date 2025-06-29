"""
SEC API Client for fetching financial data from SEC EDGAR database.
Handles rate limiting, retries, and data parsing.
"""

import asyncio
import aiohttp
import json
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import hashlib

from .config import get_config
from .models import CompanyInfo, FinancialFact, UnitType, FormType


class RateLimiter:
    """Rate limiter for SEC API requests."""
    
    def __init__(self, requests_per_second: int = 10):
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """Acquire rate limit permission."""
        async with self._lock:
            now = time.time()
            time_since_last = now - self.last_request_time
            
            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last
                await asyncio.sleep(sleep_time)
            
            self.last_request_time = time.time()


class SECAPIClient:
    """Client for interacting with SEC EDGAR API."""
    
    def __init__(self):
        self.config = get_config()
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limiter = RateLimiter(
            self.config.sec_api.rate_limit.get("requests_per_second", 10)
        )
        self.logger = logging.getLogger(__name__)
        
        # Cache for ticker-to-CIK mapping
        self._ticker_to_cik_cache: Dict[str, str] = {}
        self._cache_last_updated: Optional[datetime] = None
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close_session()
    
    async def start_session(self):
        """Start the aiohttp session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.config.sec_api.timeout)
            connector = aiohttp.TCPConnector(limit=10)
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={"User-Agent": self.config.sec_api.user_agent}
            )
    
    async def close_session(self):
        """Close the aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def _make_request(self, url: str, retries: int = None) -> Dict[str, Any]:
        """Make a rate-limited HTTP request with retries."""
        if retries is None:
            retries = self.config.sec_api.retry_attempts
        
        await self.rate_limiter.acquire()
        
        for attempt in range(retries + 1):
            try:
                await self.start_session()  # Ensure session is active
                
                self.logger.debug(f"Making request to: {url} (attempt {attempt + 1})")
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    elif response.status == 429:  # Rate limited
                        wait_time = 2 ** attempt  # Exponential backoff
                        self.logger.warning(f"Rate limited, waiting {wait_time}s before retry")
                        await asyncio.sleep(wait_time)
                        continue
                    elif response.status == 404:
                        raise FileNotFoundError(f"Data not found for URL: {url}")
                    else:
                        response.raise_for_status()
                        
            except aiohttp.ClientError as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt == retries:
                    raise
                await asyncio.sleep(self.config.sec_api.retry_delay * (attempt + 1))
        
        raise Exception(f"Failed to fetch data after {retries + 1} attempts")
    
    async def get_company_tickers(self) -> Dict[str, CompanyInfo]:
        """Fetch the company tickers mapping from SEC."""
        url = self.config.sec_api.tickers_url
        
        try:
            data = await self._make_request(url)
            
            companies = {}
            # Handle both old and new format of SEC tickers file
            if 'fields' in data and 'data' in data:
                # New format with fields array
                field_names = data['fields']
                for row in data['data']:
                    row_dict = dict(zip(field_names, row))
                    ticker = row_dict.get('ticker', '').upper()
                    if ticker:
                        companies[ticker] = CompanyInfo(
                            ticker=ticker,
                            cik=str(row_dict.get('cik_str', '') or '').zfill(10),
                            name=row_dict.get('title', '')
                        )
            else:
                # Handle indexed format like {'0': {...}, '1': {...}}
                for key, company_data in data.items():
                    if isinstance(company_data, dict) and 'ticker' in company_data:
                        ticker = company_data['ticker'].upper()
                        companies[ticker] = CompanyInfo(
                            ticker=ticker,
                            cik=str(company_data.get('cik_str', '') or '').zfill(10),
                            name=company_data.get('title', '')
                        )
            
            # Update cache
            self._ticker_to_cik_cache = {
                ticker: info.cik for ticker, info in companies.items()
            }
            self._cache_last_updated = datetime.utcnow()
            
            self.logger.info(f"Loaded {len(companies)} company tickers from SEC")
            return companies
            
        except Exception as e:
            self.logger.error(f"Failed to fetch company tickers: {e}")
            raise
    
    async def get_cik_for_ticker(self, ticker: str) -> str:
        """Get CIK for a given ticker symbol."""
        ticker = ticker.upper()
        
        # Check cache first
        if (ticker in self._ticker_to_cik_cache and 
            self._cache_last_updated and 
            datetime.utcnow() - self._cache_last_updated < timedelta(hours=24)):
            return self._ticker_to_cik_cache[ticker]
        
        # Refresh cache
        companies = await self.get_company_tickers()
        
        if ticker not in companies:
            raise ValueError(f"Ticker '{ticker}' not found in SEC database")
        
        return companies[ticker].cik
    
    async def get_company_facts(self, ticker: str) -> Dict[str, Any]:
        """Fetch company facts data from SEC for a given ticker."""
        try:
            cik = await self.get_cik_for_ticker(ticker)
            url = f"{self.config.sec_api.base_url}/CIK{cik}.json"
            
            self.logger.info(f"Fetching company facts for {ticker} (CIK: {cik})")
            data = await self._make_request(url)
            
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to fetch company facts for {ticker}: {e}")
            raise
    
    def parse_company_facts(self, raw_data: Dict[str, Any], ticker: str) -> Tuple[CompanyInfo, List[FinancialFact]]:
        """Parse raw SEC company facts data into structured models."""
        facts = []
        
        # Extract company info
        entity_name = raw_data.get('entityName', '')
        cik = str(raw_data.get('cik', '') or '').zfill(10)
        
        company_info = CompanyInfo(
            ticker=ticker.upper(),
            cik=cik,
            name=entity_name
        )
        
        # Parse facts from different taxonomies
        fact_data = raw_data.get('facts', {})
        
        for taxonomy, taxonomy_data in fact_data.items():
            self.logger.debug(f"Processing taxonomy: {taxonomy}")
            
            for concept, concept_data in taxonomy_data.items():
                label = concept_data.get('label', concept)
                description = concept_data.get('description', '')
                
                # Process each unit type (USD, shares, etc.)
                units_data = concept_data.get('units', {})
                
                for unit, unit_facts in units_data.items():
                    # Map unit to our enum
                    try:
                        unit_type = UnitType(unit) if unit in [e.value for e in UnitType] else None
                    except ValueError:
                        unit_type = None
                    
                    for fact_entry in unit_facts:
                        try:
                            fact = FinancialFact(
                                label=label,
                                description=description,
                                value=fact_entry.get('val'),
                                unit=unit_type,
                                start_date=self._parse_date(fact_entry.get('start')),
                                end_date=self._parse_date(fact_entry.get('end')),
                                instant_date=self._parse_date(fact_entry.get('instant')),
                                form=self._parse_form_type(fact_entry.get('form')),
                                fiscal_year=fact_entry.get('fy'),
                                fiscal_period=fact_entry.get('fp'),
                                frame=fact_entry.get('frame')
                            )
                            facts.append(fact)
                            
                        except Exception as e:
                            self.logger.warning(f"Failed to parse fact entry: {e}")
                            continue
        
        self.logger.info(f"Parsed {len(facts)} facts for {ticker}")
        return company_info, facts
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime.date]:
        """Parse date string to date object."""
        if not date_str:
            return None
        
        try:
            return datetime.fromisoformat(date_str).date()
        except (ValueError, TypeError):
            return None
    
    def _parse_form_type(self, form_str: Optional[str]) -> Optional[FormType]:
        """Parse form type string to FormType enum."""
        if not form_str:
            return None
        
        form_mapping = {
            '10-K': FormType.FORM_10K,
            '10-Q': FormType.FORM_10Q,
            '8-K': FormType.FORM_8K
        }
        
        return form_mapping.get(form_str)
    
    async def fetch_company_data(self, ticker: str) -> Tuple[CompanyInfo, List[FinancialFact]]:
        """High-level method to fetch and parse company data."""
        raw_data = await self.get_company_facts(ticker)
        return self.parse_company_facts(raw_data, ticker)
    
    async def batch_fetch_companies(self, tickers: List[str]) -> Dict[str, Tuple[CompanyInfo, List[FinancialFact]]]:
        """Fetch data for multiple companies with controlled concurrency."""
        max_concurrent = self.config.etl.max_concurrent_downloads
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_single(ticker: str):
            async with semaphore:
                try:
                    return ticker, await self.fetch_company_data(ticker)
                except Exception as e:
                    self.logger.error(f"Failed to fetch data for {ticker}: {e}")
                    return ticker, None
        
        # Create tasks for all tickers
        tasks = [fetch_single(ticker) for ticker in tickers]
        
        # Execute with progress logging
        results = {}
        completed = 0
        total = len(tasks)
        
        for coro in asyncio.as_completed(tasks):
            ticker, data = await coro
            completed += 1
            
            if data:
                results[ticker] = data
                self.logger.info(f"Completed {ticker} ({completed}/{total})")
            else:
                self.logger.warning(f"Failed to fetch {ticker} ({completed}/{total})")
        
        return results
    
    def calculate_content_hash(self, data: Dict[str, Any]) -> str:
        """Calculate hash of content for change detection."""
        content_str = json.dumps(data, sort_keys=True)
        return hashlib.md5(content_str.encode()).hexdigest()


# Helper functions for external use

async def create_sec_client() -> SECAPIClient:
    """Create and initialize SEC API client."""
    client = SECAPIClient()
    await client.start_session()
    return client


async def fetch_ticker_data(ticker: str) -> Tuple[CompanyInfo, List[FinancialFact]]:
    """Convenience function to fetch data for a single ticker."""
    async with SECAPIClient() as client:
        return await client.fetch_company_data(ticker)


async def fetch_multiple_tickers(tickers: List[str]) -> Dict[str, Tuple[CompanyInfo, List[FinancialFact]]]:
    """Convenience function to fetch data for multiple tickers."""
    async with SECAPIClient() as client:
        return await client.batch_fetch_companies(tickers)


if __name__ == "__main__":
    # Test the SEC client
    async def test():
        async with SECAPIClient() as client:
            # Test single company fetch
            company_info, facts = await client.fetch_company_data("AAPL")
            print(f"Fetched {len(facts)} facts for {company_info.name}")
            
            # Test batch fetch
            results = await client.batch_fetch_companies(["MSFT", "GOOGL"])
            for ticker, data in results.items():
                if data:
                    company_info, facts = data
                    print(f"{ticker}: {len(facts)} facts")
    
    asyncio.run(test()) 