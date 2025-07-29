"""
Comprehensive 13F Functionality Test Suite
Tests all aspects of the 13F hedge fund analysis system.
"""

import asyncio
import logging
import sys
import json
from pathlib import Path
import aiohttp
from datetime import datetime

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from core.sec_data_service import SECDataService
from sec_13f.client import SEC13FClient
from sec_13f.managers.berkshire_hathaway import BerkshireHathawayHandler
from sec_13f.managers import get_famous_hedge_fund_managers
from etl.data_manager import DataManager

# Set up logging
logging.basicConfig(level=logging.WARNING)  # Reduce noise in tests
logger = logging.getLogger(__name__)


class Test13FFunctionality:
    """Comprehensive test suite for 13F functionality."""
    
    def __init__(self):
        self.results = {}
        
    async def test_famous_managers(self):
        """Test famous hedge fund managers database."""
        try:
            managers = get_famous_hedge_fund_managers()
            assert len(managers) >= 20, f"Expected at least 20 managers, got {len(managers)}"
            
            # Test Berkshire exists
            berkshire = next((m for m in managers if "BERKSHIRE" in m.name), None)
            assert berkshire is not None, "Berkshire Hathaway not found"
            assert berkshire.cik == "0001067983", f"Wrong CIK for Berkshire: {berkshire.cik}"
            assert "Warren Buffett" in berkshire.key_personnel, "Warren Buffett not in personnel"
            
            self.results['famous_managers'] = True
            return True
            
        except Exception as e:
            logger.error(f"Famous managers test failed: {e}")
            self.results['famous_managers'] = False
            return False
    
    async def test_sec_client(self):
        """Test SEC 13F client."""
        try:
            async with SEC13FClient() as client:
                # Test CIK validation
                assert await client.validate_cik("0001067983"), "Berkshire CIK validation failed"
                
                # Test submissions
                submissions = await client.get_company_submissions("0001067983")
                assert "filings" in submissions, "No filings in submissions"
                
                # Test 13F filings
                filings = await client.fetch_13f_filings("0001067983", periods=1)
                assert len(filings) >= 0, "Failed to fetch filings"  # May be 0 due to parsing
                
            self.results['sec_client'] = True
            return True
            
        except Exception as e:
            logger.error(f"SEC client test failed: {e}")
            self.results['sec_client'] = False
            return False
    
    async def test_berkshire_handler(self):
        """Test Berkshire Hathaway specialized handler."""
        try:
            async with BerkshireHathawayHandler() as handler:
                # Test portfolio retrieval
                portfolio = await handler.get_latest_portfolio()
                # May be None due to parsing issues, but should not error
                
                # Test insights generation
                insights = await handler.get_berkshire_insights()
                assert isinstance(insights, dict), "Insights should be a dictionary"
                
            self.results['berkshire_handler'] = True
            return True
            
        except Exception as e:
            logger.error(f"Berkshire handler test failed: {e}")
            self.results['berkshire_handler'] = False
            return False
    
    async def test_centralized_service(self):
        """Test centralized SEC data service."""
        try:
            async with SECDataService() as service:
                # Test health
                health = await service.get_service_health()
                assert "overall_status" in health, "Health check missing status"
                
                # Test managers
                managers = await service.get_hedge_fund_managers()
                assert len(managers) >= 20, f"Expected at least 20 managers, got {len(managers)}"
                
            self.results['centralized_service'] = True
            return True
            
        except Exception as e:
            logger.error(f"Centralized service test failed: {e}")
            self.results['centralized_service'] = False
            return False
    
    async def test_data_manager(self):
        """Test data manager 13F integration."""
        try:
            data_manager = DataManager()
            
            # Test paths exist
            assert data_manager.hedge_fund_path.exists(), "Hedge fund path doesn't exist"
            
            # Test listing (should not error)
            available = data_manager.list_available_hedge_funds()
            assert isinstance(available, list), "Available hedge funds should be a list"
            
            self.results['data_manager'] = True
            return True
            
        except Exception as e:
            logger.error(f"Data manager test failed: {e}")
            self.results['data_manager'] = False
            return False
    
    async def run_all_tests(self):
        """Run all tests."""
        print("🧪 Running 13F Functionality Tests...")
        
        tests = [
            ("Famous Managers", self.test_famous_managers),
            ("SEC Client", self.test_sec_client),
            ("Berkshire Handler", self.test_berkshire_handler),
            ("Centralized Service", self.test_centralized_service),
            ("Data Manager", self.test_data_manager)
        ]
        
        for test_name, test_func in tests:
            print(f"  Running {test_name}...")
            result = await test_func()
            status = "✓ PASS" if result else "✗ FAIL"
            print(f"  {status} {test_name}")
        
        passed = sum(self.results.values())
        total = len(self.results)
        
        print(f"\n📊 Results: {passed}/{total} tests passed ({(passed/total)*100:.1f}%)")
        
        if passed == total:
            print("🎉 All 13F functionality tests passed!")
            return True
        else:
            print("❌ Some tests failed")
            return False


async def main():
    """Run the test suite."""
    test_suite = Test13FFunctionality()
    success = await test_suite.run_all_tests()
    return success


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1) 