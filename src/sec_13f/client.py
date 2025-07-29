"""
SEC 13F Filing Client
Handles fetching and parsing of institutional investment manager 13F filings from SEC EDGAR.
"""

import asyncio
import aiohttp
import json
import logging
import re
from typing import Dict, List, Optional, Any
from datetime import datetime, date, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET
from urllib.parse import urljoin

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from core.config import get_config
from core.models import (
    Form13F, HedgeFundManager, InstitutionalHolding, FilingType, HoldingType
)
# Import will be done locally to avoid circular imports


class SEC13FClient:
    """Client for fetching 13F filings from SEC EDGAR."""
    
    def __init__(self):
        self.config = get_config()
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger(__name__)
        
        # SEC EDGAR endpoints
        self.edgar_base_url = "https://www.sec.gov/Archives/edgar/data"
        self.company_search_url = "https://www.sec.gov/cgi-bin/browse-edgar"
        self.submissions_url = "https://data.sec.gov/submissions"
        
        # Rate limiting (SEC allows 10 requests per second)
        self._last_request_time = 0.0
        self._min_interval = 0.1  # 100ms between requests
        
    async def start_session(self):
        """Start the aiohttp session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(limit=10)
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    "User-Agent": self.config.sec_api.user_agent,
                    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                    "Pragma": "no-cache",
                    "Cache-Control": "no-cache"
                }
            )
    
    async def close_session(self):
        """Close the aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close_session()
    
    async def _rate_limited_request(self, url: str) -> aiohttp.ClientResponse:
        """Make a rate-limited request to SEC."""
        # Ensure we don't exceed rate limits
        now = asyncio.get_event_loop().time()
        time_since_last = now - self._last_request_time
        
        if time_since_last < self._min_interval:
            await asyncio.sleep(self._min_interval - time_since_last)
        
        await self.start_session()
        
        self.logger.debug(f"Making request to: {url}")
        response = await self.session.get(url)
        self._last_request_time = asyncio.get_event_loop().time()
        
        return response
    
    async def get_company_submissions(self, cik: str) -> Dict[str, Any]:
        """Get company submission history from SEC."""
        cik = cik.zfill(10)
        url = f"{self.submissions_url}/CIK{cik}.json"
        
        response = await self._rate_limited_request(url)
        
        if response.status == 200:
            return await response.json()
        elif response.status == 404:
            raise ValueError(f"No submissions found for CIK {cik}")
        else:
            response.raise_for_status()
    
    async def fetch_13f_filings(self, cik: str, periods: Optional[int] = 4) -> List[Form13F]:
        """Fetch 13F filings for a given CIK."""
        try:
            submissions = await self.get_company_submissions(cik)
            
            # Find 13F filings
            filings_data = submissions.get('filings', {}).get('recent', {})
            
            if not filings_data:
                self.logger.warning(f"No recent filings found for CIK {cik}")
                return []
            
            forms = filings_data.get('form', [])
            accession_numbers = filings_data.get('accessionNumber', [])
            filing_dates = filings_data.get('filingDate', [])
            report_dates = filings_data.get('reportDate', [])
            
            # Filter for 13F filings
            form_13f_indices = [
                i for i, form in enumerate(forms) 
                if form in ['13F-HR', '13F-NT', '13F-HR/A']
            ]
            
            if not form_13f_indices:
                self.logger.warning(f"No 13F filings found for CIK {cik}")
                return []
            
            # Limit to requested number of periods
            if periods:
                form_13f_indices = form_13f_indices[:periods]
            
            # Fetch detailed filing data
            filings = []
            for i in form_13f_indices:
                try:
                    filing = await self._fetch_13f_filing_detail(
                        cik=cik,
                        accession_number=accession_numbers[i],
                        form_type=forms[i],
                        filing_date=filing_dates[i],
                        report_date=report_dates[i] if i < len(report_dates) else None
                    )
                    if filing:
                        filings.append(filing)
                        
                except Exception as e:
                    self.logger.error(f"Failed to fetch 13F filing {accession_numbers[i]}: {e}")
                    continue
            
            return filings
            
        except Exception as e:
            self.logger.error(f"Failed to fetch 13F filings for CIK {cik}: {e}")
            raise
    
    async def _fetch_13f_filing_detail(
        self, 
        cik: str, 
        accession_number: str, 
        form_type: str,
        filing_date: str,
        report_date: Optional[str] = None
    ) -> Optional[Form13F]:
        """Fetch detailed 13F filing data."""
        try:
            # Clean accession number for URL
            clean_accession = accession_number.replace('-', '')
            
            # Construct filing URL
            filing_url = f"{self.edgar_base_url}/{cik.zfill(10)}/{clean_accession}/{accession_number}.txt"
            
            response = await self._rate_limited_request(filing_url)
            
            if response.status != 200:
                self.logger.warning(f"Failed to fetch filing {accession_number}: HTTP {response.status}")
                return None
            
            filing_text = await response.text()
            
            # Parse the filing
            return await self._parse_13f_filing(
                filing_text=filing_text,
                cik=cik,
                accession_number=accession_number,
                form_type=form_type,
                filing_date=filing_date,
                report_date=report_date
            )
            
        except Exception as e:
            self.logger.error(f"Error fetching 13F detail for {accession_number}: {e}")
            return None
    
    async def _parse_13f_filing(
        self,
        filing_text: str,
        cik: str,
        accession_number: str,
        form_type: str,
        filing_date: str,
        report_date: Optional[str] = None
    ) -> Optional[Form13F]:
        """Parse 13F filing text into structured data."""
        try:
            # Find the information table in the filing
            holdings = []
            manager_name = "Unknown Manager"
            total_value = 0.0
            
            # Look for XML section or tabular data
            # This is a simplified parser - real 13F filings can be complex
            
            # Try to find manager name
            name_match = re.search(r'COMPANY CONFORMED NAME:\s+(.+)', filing_text, re.IGNORECASE)
            if name_match:
                manager_name = name_match.group(1).strip()
            
            # Try to find holdings data (this is simplified)
            # Real implementation would need to handle various formats
            holdings_section = self._extract_holdings_section(filing_text)
            
            if holdings_section:
                holdings = self._parse_holdings_data(holdings_section)
                total_value = sum(holding.value for holding in holdings)
            
            # Parse dates
            filing_date_obj = datetime.strptime(filing_date, '%Y-%m-%d').date()
            
            if report_date:
                period_end_date = datetime.strptime(report_date, '%Y-%m-%d').date()
            else:
                # Assume quarter end based on filing date
                period_end_date = self._estimate_period_end_date(filing_date_obj)
            
            # Create Form13F object
            form_13f = Form13F(
                filing_manager_cik=cik,
                filing_manager_name=manager_name,
                filing_id=accession_number,
                filing_type=FilingType(form_type) if form_type in [e.value for e in FilingType] else FilingType.FORM_13F_HR,
                period_end_date=period_end_date,
                filing_date=filing_date_obj,
                total_holdings_count=len(holdings),
                total_value=total_value,
                holdings=holdings,
                amendment='A' in form_type,
                sec_url=f"https://www.sec.gov/Archives/edgar/data/{cik.zfill(10)}/{accession_number.replace('-', '')}/{accession_number}-index.html"
            )
            
            return form_13f
            
        except Exception as e:
            self.logger.error(f"Error parsing 13F filing {accession_number}: {e}")
            return None
    
    def _extract_holdings_section(self, filing_text: str) -> Optional[str]:
        """Extract the holdings information table from filing text."""
        # Look for common patterns in 13F filings
        patterns = [
            # Look for INFORMATION TABLE document (most common for modern filings)
            r'<TYPE>INFORMATION TABLE.*?<TEXT>\s*<XML>(.*?)</XML>',
            # Look for informationTable XML element
            r'<informationTable[^>]*>(.*?)</informationTable>',
            # Look for text-based information table sections
            r'INFORMATION TABLE(.+?)(?=SIGNATURE|</DOCUMENT>|$)',
            r'Table I -- INFORMATION TABLE(.+?)(?=SIGNATURE|</DOCUMENT>|$)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filing_text, re.DOTALL | re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def _parse_holdings_data(self, holdings_section: str) -> List[InstitutionalHolding]:
        """Parse holdings from the extracted section."""
        holdings = []
        
        # Try XML format first
        try:
            if '<' in holdings_section and '>' in holdings_section:
                holdings = self._parse_xml_holdings(holdings_section)
                if holdings:
                    self.logger.debug(f"XML parsing successful: {len(holdings)} holdings")
                    return holdings
        except Exception as e:
            self.logger.debug(f"XML parsing failed: {e}")
        
        # If XML parsing didn't work, try regex parsing for <infoTable> elements
        if not holdings and '<infoTable>' in holdings_section:
            self.logger.debug("Trying regex parsing for infoTable elements...")
            holdings = self._parse_xml_with_regex(holdings_section)
            if holdings:
                self.logger.debug(f"Regex parsing successful: {len(holdings)} holdings")
                return holdings
        
        # If no holdings found, try text parsing as final fallback
        if not holdings:
            holdings = self._parse_text_holdings(holdings_section)
        
        return holdings
    
    def _parse_xml_holdings(self, xml_section: str) -> List[InstitutionalHolding]:
        """Parse holdings from XML format."""
        holdings = []
        
        try:
            # Clean up XML if needed
            xml_section = xml_section.strip()
            
            # Handle case where XML doesn't have a root element
            if not xml_section.startswith('<?xml') and not xml_section.startswith('<informationTable'):
                # Check if it starts with <infoTable> directly
                if xml_section.startswith('<infoTable>'):
                    xml_section = f'<root>{xml_section}</root>'
                else:
                    xml_section = f'<informationTable>{xml_section}</informationTable>'
            
            root = ET.fromstring(xml_section)
            
            # Find holding entries - check multiple possible tag names
            holding_tags = ['infoTable', 'holdingInfo', 'holding', 'ns1:infoTable']
            
            for tag_name in holding_tags:
                # Use both direct children and descendants
                holding_elements = root.findall(f'.//{tag_name}') + root.findall(f'./{tag_name}')
                
                for holding_elem in holding_elements:
                    holding = self._parse_single_xml_holding(holding_elem)
                    if holding:
                        holdings.append(holding)
                        
                # If we found holdings with this tag, don't try others
                if holdings:
                    self.logger.debug(f"Found {len(holdings)} holdings using tag '{tag_name}'")
                    break
        
        except ET.ParseError as e:
            self.logger.debug(f"XML parsing error: {e}")
            # Try to extract basic info with regex as fallback
            holdings = self._parse_xml_with_regex(xml_section)
        except Exception as e:
            self.logger.debug(f"XML holdings parsing error: {e}")
        
        return holdings
    
    def _parse_single_xml_holding(self, elem: ET.Element) -> Optional[InstitutionalHolding]:
        """Parse a single holding from XML element."""
        try:
            # Modern 13F XML field mappings (based on Berkshire structure)
            cusip = self._get_xml_text(elem, ['cusip', 'cusip6'])
            name = self._get_xml_text(elem, ['nameOfIssuer', 'securityName', 'name'])
            title_class = self._get_xml_text(elem, ['titleOfClass', 'class'])
            value_str = self._get_xml_text(elem, ['value', 'marketValue'])
            
            # Shares can be in different nested structures
            shares_str = self._get_xml_text(elem, ['sshPrnamt', 'shares', 'sharesOrPrincipalAmount'])
            
            # Check for nested shrsOrPrnAmt structure
            if not shares_str:
                shares_elem = elem.find('.//shrsOrPrnAmt')
                if shares_elem is not None:
                    shares_str = self._get_xml_text(shares_elem, ['sshPrnamt', 'shares'])
            
            # Get voting authority
            voting_sole = self._get_xml_text(elem, ['Sole', 'sole'])
            voting_shared = self._get_xml_text(elem, ['Shared', 'shared'])
            voting_none = self._get_xml_text(elem, ['None', 'none'])
            
            # Get investment discretion
            investment_discretion = self._get_xml_text(elem, ['investmentDiscretion'])
            
            if not cusip or not name or not value_str:
                return None
            
            # Convert values - handle different number formats
            try:
                value = float(value_str.replace(',', '').replace('$', '')) if value_str else 0.0
            except ValueError:
                value = 0.0
                
            try:
                shares = int(float(shares_str.replace(',', ''))) if shares_str else 0
            except (ValueError, TypeError):
                shares = 0
            
            # Create holding
            holding = InstitutionalHolding(
                cusip=cusip,
                security_name=name,
                title_of_class=title_class,
                value=value,
                shares=shares,
                share_type=HoldingType.STOCK,
                investment_discretion=investment_discretion,
                voting_authority_sole=int(voting_sole) if voting_sole and voting_sole.isdigit() else None,
                voting_authority_shared=int(voting_shared) if voting_shared and voting_shared.isdigit() else None,
                voting_authority_none=int(voting_none) if voting_none and voting_none.isdigit() else None
            )
            
            # Calculate additional fields
            if shares > 0 and value > 0:
                holding.avg_price_per_share = (value * 1000) / shares  # Value is in thousands
            
            return holding
            
        except Exception as e:
            self.logger.debug(f"Error parsing single XML holding: {e}")
            return None
    
    def _get_xml_text(self, elem: ET.Element, field_names: List[str]) -> Optional[str]:
        """Get text from XML element trying multiple field names."""
        for field_name in field_names:
            # Try direct child first
            field_elem = elem.find(f'./{field_name}')
            if field_elem is not None and field_elem.text:
                return field_elem.text.strip()
            
            # Try descendant search
            field_elem = elem.find(f'.//{field_name}')
            if field_elem is not None and field_elem.text:
                return field_elem.text.strip()
        return None
    
    def _parse_xml_with_regex(self, xml_section: str) -> List[InstitutionalHolding]:
        """Fallback parsing using regex when XML parsing fails."""
        holdings = []
        
        try:
            # Find all infoTable entries using regex
            info_table_pattern = r'<infoTable>(.*?)</infoTable>'
            table_matches = re.findall(info_table_pattern, xml_section, re.DOTALL)
            
            for table_content in table_matches:
                # Extract fields using regex
                cusip = self._extract_regex_field(table_content, r'<cusip>([^<]+)</cusip>')
                name = self._extract_regex_field(table_content, r'<nameOfIssuer>([^<]+)</nameOfIssuer>')
                title_class = self._extract_regex_field(table_content, r'<titleOfClass>([^<]+)</titleOfClass>')
                value_str = self._extract_regex_field(table_content, r'<value>([^<]+)</value>')
                shares_str = self._extract_regex_field(table_content, r'<sshPrnamt>([^<]+)</sshPrnamt>')
                
                if cusip and name and value_str:
                    try:
                        value = float(value_str.replace(',', ''))
                        shares = int(float(shares_str.replace(',', ''))) if shares_str else 0
                        
                        holding = InstitutionalHolding(
                            cusip=cusip,
                            security_name=name,
                            title_of_class=title_class,
                            value=value,
                            shares=shares,
                            share_type=HoldingType.STOCK
                        )
                        
                        if shares > 0 and value > 0:
                            holding.avg_price_per_share = (value * 1000) / shares
                        
                        holdings.append(holding)
                        
                    except (ValueError, TypeError):
                        continue
                        
        except Exception as e:
            self.logger.debug(f"Regex parsing error: {e}")
        
        return holdings
    
    def _extract_regex_field(self, text: str, pattern: str) -> Optional[str]:
        """Extract a field using regex pattern."""
        match = re.search(pattern, text, re.IGNORECASE)
        return match.group(1).strip() if match else None
    
    def _parse_text_holdings(self, text_section: str) -> List[InstitutionalHolding]:
        """Parse holdings from text format (fallback)."""
        # This is a very basic text parser
        # Real implementation would need sophisticated pattern matching
        holdings = []
        
        lines = text_section.split('\n')
        for line in lines:
            # Look for lines with potential CUSIP patterns
            if re.search(r'\d{6,9}', line):  # Basic CUSIP pattern
                # Try to extract basic info (very simplified)
                parts = line.split()
                if len(parts) >= 3:
                    # This is just a placeholder - real parsing is much more complex
                    pass
        
        return holdings
    
    def _estimate_period_end_date(self, filing_date: date) -> date:
        """Estimate period end date based on filing date."""
        # 13F filings are due 45 days after quarter end
        # So we can estimate the quarter end
        
        filing_month = filing_date.month
        filing_year = filing_date.year
        
        # Typical filing windows
        if filing_month <= 2:  # Filed in Jan-Feb, likely Q4 of previous year
            return date(filing_year - 1, 12, 31)
        elif filing_month <= 5:  # Filed in Mar-May, likely Q1
            return date(filing_year, 3, 31)
        elif filing_month <= 8:  # Filed in Jun-Aug, likely Q2
            return date(filing_year, 6, 30)
        else:  # Filed in Sep-Nov, likely Q3
            return date(filing_year, 9, 30)
    
    async def fetch_latest_13f_filing(self, cik: str) -> Optional[Form13F]:
        """Get the most recent 13F filing for a manager."""
        filings = await self.fetch_13f_filings(cik, periods=1)
        return filings[0] if filings else None
    
    async def get_famous_hedge_fund_managers(self) -> List[HedgeFundManager]:
        """Get list of famous hedge fund managers."""
        # Import locally to avoid circular imports
        from sec_13f.managers import get_famous_hedge_fund_managers
        return get_famous_hedge_fund_managers()
    
    async def get_manager_by_cik(self, cik: str) -> Optional[HedgeFundManager]:
        """Get manager information by CIK."""
        managers = await self.get_famous_hedge_fund_managers()
        return next((m for m in managers if m.cik == cik.zfill(10)), None)
    
    async def validate_cik(self, cik: str) -> bool:
        """Validate that a CIK exists and has filings."""
        try:
            submissions = await self.get_company_submissions(cik)
            return bool(submissions.get('filings', {}).get('recent', {}))
        except Exception:
            return False


# Convenience functions

async def fetch_hedge_fund_portfolio(cik: str, periods: int = 4) -> List[Form13F]:
    """Convenience function to fetch 13F portfolio."""
    async with SEC13FClient() as client:
        return await client.fetch_13f_filings(cik, periods)


async def get_latest_portfolio(cik: str) -> Optional[Form13F]:
    """Convenience function to get latest 13F filing."""
    async with SEC13FClient() as client:
        return await client.fetch_latest_13f_filing(cik)


if __name__ == "__main__":
    # Test the client
    async def test():
        async with SEC13FClient() as client:
            # Test with Berkshire Hathaway
            print("Testing 13F client with Berkshire Hathaway (CIK: 0001067983)...")
            
            try:
                filings = await client.fetch_13f_filings("0001067983", periods=2)
                print(f"Found {len(filings)} 13F filings")
                
                for filing in filings:
                    print(f"Filing: {filing.filing_id}, Date: {filing.filing_date}, Holdings: {filing.total_holdings_count}")
                    
            except Exception as e:
                print(f"Error testing 13F client: {e}")
    
    asyncio.run(test()) 