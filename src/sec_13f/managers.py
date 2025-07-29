"""
Famous Hedge Fund Managers Database
Contains information about well-known institutional investment managers.
"""

from typing import List
from datetime import date
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from core.models import HedgeFundManager


# Famous hedge fund managers and their information
FAMOUS_MANAGERS = [
    {
        "name": "BERKSHIRE HATHAWAY INC",
        "cik": "0001067983",
        "common_name": "Berkshire Hathaway",
        "manager_type": "investment_company",
        "address": "1440 KIEWIT PLAZA",
        "city": "OMAHA",
        "state": "NE",
        "zip_code": "68131",
        "key_personnel": ["Warren Buffett", "Charlie Munger"],
        "description": "Warren Buffett's legendary investment company"
    },
    {
        "name": "BRIDGEWATER ASSOCIATES LP",
        "cik": "0001350694",
        "common_name": "Bridgewater Associates",
        "manager_type": "hedge_fund",
        "address": "One Glendinning Place",
        "city": "Westport",
        "state": "CT",
        "zip_code": "06880",
        "key_personnel": ["Ray Dalio"],
        "description": "World's largest hedge fund, founded by Ray Dalio"
    },
    {
        "name": "RENAISSANCE TECHNOLOGIES LLC",
        "cik": "0001037389",
        "common_name": "Renaissance Technologies",
        "manager_type": "hedge_fund",
        "address": "800 North Country Road",
        "city": "East Setauket",
        "state": "NY",
        "zip_code": "11733",
        "key_personnel": ["James Simons", "Peter Brown"],
        "description": "Quantitative trading pioneer, known for the Medallion Fund"
    },
    {
        "name": "CITADEL ADVISORS LLC",
        "cik": "0001423053",
        "common_name": "Citadel",
        "manager_type": "hedge_fund",
        "address": "131 S. Dearborn Street",
        "city": "Chicago",
        "state": "IL",
        "zip_code": "60603",
        "key_personnel": ["Ken Griffin"],
        "description": "Multi-strategy hedge fund founded by Ken Griffin"
    },
    {
        "name": "GREENLIGHT CAPITAL INC",
        "cik": "0001079114",
        "common_name": "Greenlight Capital",
        "manager_type": "hedge_fund",
        "address": "140 E 45TH ST 24TH FL",
        "city": "NEW YORK",
        "state": "NY",
        "zip_code": "10017",
        "key_personnel": ["David Einhorn"],
        "description": "Value-oriented hedge fund run by David Einhorn"
    },
    {
        "name": "PERSHING SQUARE CAPITAL MANAGEMENT, L.P.",
        "cik": "0001336528",
        "common_name": "Pershing Square",
        "manager_type": "hedge_fund",
        "address": "787 Eleventh Avenue",
        "city": "New York",
        "state": "NY",
        "zip_code": "10019",
        "key_personnel": ["Bill Ackman"],
        "description": "Activist hedge fund led by William Ackman"
    },
    {
        "name": "THIRD POINT LLC",
        "cik": "0001040273",
        "common_name": "Third Point",
        "manager_type": "hedge_fund",
        "address": "390 Park Avenue",
        "city": "New York",
        "state": "NY",
        "zip_code": "10022",
        "key_personnel": ["Daniel Loeb"],
        "description": "Event-driven hedge fund run by Dan Loeb"
    },
    {
        "name": "TIGER GLOBAL MANAGEMENT LLC",
        "cik": "0001167483",
        "common_name": "Tiger Global",
        "manager_type": "hedge_fund",
        "address": "9 West 57th Street",
        "city": "New York",
        "state": "NY",
        "zip_code": "10019",
        "key_personnel": ["Chase Coleman"],
        "description": "Technology-focused hedge fund led by Chase Coleman"
    },
    {
        "name": "COATUE MANAGEMENT LLC",
        "cik": "0001482298",
        "common_name": "Coatue Management",
        "manager_type": "hedge_fund",
        "address": "9 West 57th Street",
        "city": "New York",
        "state": "NY",
        "zip_code": "10019",
        "key_personnel": ["Philippe Laffont"],
        "description": "Technology, media, and telecommunications focused fund"
    },
    {
        "name": "JANA PARTNERS LLC",
        "cik": "0001159159",
        "common_name": "Jana Partners",
        "manager_type": "hedge_fund",
        "address": "787 Seventh Avenue",
        "city": "New York",
        "state": "NY",
        "zip_code": "10019",
        "key_personnel": ["Barry Rosenstein"],
        "description": "Event-driven activist investment firm"
    },
    {
        "name": "ICAHN ASSOCIATES CORP",
        "cik": "0000921669",
        "common_name": "Icahn Enterprises",
        "manager_type": "activist_investor",
        "address": "767 Fifth Avenue",
        "city": "New York",
        "state": "NY",
        "zip_code": "10153",
        "key_personnel": ["Carl Icahn"],
        "description": "Carl Icahn's activist investment vehicle"
    },
    {
        "name": "ELLIOTT INVESTMENT MANAGEMENT L.P.",
        "cik": "0001067983",  # Note: This might not be the correct CIK
        "common_name": "Elliott Management",
        "manager_type": "hedge_fund",
        "address": "40 West 57th Street",
        "city": "New York",
        "state": "NY",
        "zip_code": "10019",
        "key_personnel": ["Paul Singer"],
        "description": "Multi-strategy activist hedge fund"
    },
    {
        "name": "POINT72 ASSET MANAGEMENT, L.P.",
        "cik": "0001603466",
        "common_name": "Point72",
        "manager_type": "hedge_fund",
        "address": "72 Cummings Point Road",
        "city": "Stamford",
        "state": "CT",
        "zip_code": "06902",
        "key_personnel": ["Steven Cohen"],
        "description": "Multi-manager hedge fund led by Steve Cohen"
    },
    {
        "name": "D. E. SHAW & CO., L.P.",
        "cik": "0000928645",
        "common_name": "D.E. Shaw",
        "manager_type": "hedge_fund",
        "address": "1166 Avenue of the Americas",
        "city": "New York",
        "state": "NY",
        "zip_code": "10036",
        "key_personnel": ["David Shaw"],
        "description": "Quantitative investment management firm"
    },
    {
        "name": "BAUPOST GROUP LLC/MA",
        "cik": "0001061768",
        "common_name": "Baupost Group",
        "manager_type": "hedge_fund",
        "address": "10 St. James Avenue",
        "city": "Boston",
        "state": "MA",
        "zip_code": "02116",
        "key_personnel": ["Seth Klarman"],
        "description": "Value investing firm led by Seth Klarman"
    },
    {
        "name": "MILLENNIUM MANAGEMENT LLC",
        "cik": "0001105028",
        "common_name": "Millennium Management",
        "manager_type": "hedge_fund",
        "address": "666 Fifth Avenue",
        "city": "New York",
        "state": "NY",
        "zip_code": "10103",
        "key_personnel": ["Israel Englander"],
        "description": "Multi-strategy hedge fund platform"
    },
    {
        "name": "TWO SIGMA INVESTMENTS, LP",
        "cik": "0001633917",
        "common_name": "Two Sigma",
        "manager_type": "hedge_fund",
        "address": "100 Avenue of the Americas",
        "city": "New York",
        "state": "NY",
        "zip_code": "10013",
        "key_personnel": ["John Overdeck", "David Siegel"],
        "description": "Technology-driven investment manager"
    },
    {
        "name": "VIKING GLOBAL INVESTORS LP",
        "cik": "0001103804",
        "common_name": "Viking Global",
        "manager_type": "hedge_fund",
        "address": "55 Railroad Avenue",
        "city": "Greenwich",
        "state": "CT",
        "zip_code": "06830",
        "key_personnel": ["Andreas Halvorsen"],
        "description": "Long/short equity hedge fund"
    },
    {
        "name": "LONE PINE CAPITAL LLC",
        "cik": "0001061165",
        "common_name": "Lone Pine Capital",
        "manager_type": "hedge_fund",
        "address": "2 Greenwich Plaza",
        "city": "Greenwich",
        "state": "CT",
        "zip_code": "06830",
        "key_personnel": ["Stephen Mandel"],
        "description": "Long/short equity investment firm"
    },
    {
        "name": "SOROS FUND MANAGEMENT LLC",
        "cik": "0001029160",
        "common_name": "Soros Fund Management",
        "manager_type": "hedge_fund",
        "address": "250 West 55th Street",
        "city": "New York",
        "state": "NY",
        "zip_code": "10019",
        "key_personnel": ["George Soros"],
        "description": "George Soros's investment management firm"
    }
]


def get_famous_hedge_fund_managers() -> List[HedgeFundManager]:
    """Get list of famous hedge fund managers as HedgeFundManager objects."""
    managers = []
    
    for manager_data in FAMOUS_MANAGERS:
        manager = HedgeFundManager(
            name=manager_data["name"],
            cik=manager_data["cik"],
            common_name=manager_data.get("common_name"),
            manager_type=manager_data.get("manager_type", "investment_manager"),
            address=manager_data.get("address"),
            city=manager_data.get("city"),
            state=manager_data.get("state"),
            zip_code=manager_data.get("zip_code"),
            key_personnel=manager_data.get("key_personnel", [])
        )
        managers.append(manager)
    
    return managers


def get_manager_by_name(name: str) -> HedgeFundManager:
    """Get a manager by name (case-insensitive search)."""
    name_lower = name.lower()
    
    for manager_data in FAMOUS_MANAGERS:
        if (name_lower in manager_data["name"].lower() or 
            name_lower in manager_data.get("common_name", "").lower()):
            return HedgeFundManager(
                name=manager_data["name"],
                cik=manager_data["cik"],
                common_name=manager_data.get("common_name"),
                manager_type=manager_data.get("manager_type", "investment_manager"),
                address=manager_data.get("address"),
                city=manager_data.get("city"),
                state=manager_data.get("state"),
                zip_code=manager_data.get("zip_code"),
                key_personnel=manager_data.get("key_personnel", [])
            )
    
    raise ValueError(f"Manager '{name}' not found in famous managers database")


def get_manager_by_cik(cik: str) -> HedgeFundManager:
    """Get a manager by CIK."""
    cik_padded = cik.zfill(10)
    
    for manager_data in FAMOUS_MANAGERS:
        if manager_data["cik"] == cik_padded:
            return HedgeFundManager(
                name=manager_data["name"],
                cik=manager_data["cik"],
                common_name=manager_data.get("common_name"),
                manager_type=manager_data.get("manager_type", "investment_manager"),
                address=manager_data.get("address"),
                city=manager_data.get("city"),
                state=manager_data.get("state"),
                zip_code=manager_data.get("zip_code"),
                key_personnel=manager_data.get("key_personnel", [])
            )
    
    raise ValueError(f"Manager with CIK '{cik}' not found in famous managers database")


# Convenience dictionaries for quick lookups
MANAGERS_BY_CIK = {data["cik"]: data for data in FAMOUS_MANAGERS}
MANAGERS_BY_COMMON_NAME = {data["common_name"].lower(): data for data in FAMOUS_MANAGERS if data.get("common_name")}


if __name__ == "__main__":
    # Test the managers module
    print("Famous Hedge Fund Managers:")
    print("-" * 50)
    
    managers = get_famous_hedge_fund_managers()
    for manager in managers:
        print(f"{manager.common_name or manager.name}")
        print(f"  CIK: {manager.cik}")
        print(f"  Type: {manager.manager_type}")
        if manager.key_personnel:
            print(f"  Key Personnel: {', '.join(manager.key_personnel)}")
        print()
    
    print(f"\nTotal managers: {len(managers)}")
    
    # Test lookup functions
    print("\nTesting lookup functions:")
    try:
        berkshire = get_manager_by_name("berkshire")
        print(f"Found by name: {berkshire.common_name}")
        
        soros = get_manager_by_cik("0001029160")
        print(f"Found by CIK: {soros.common_name}")
        
    except ValueError as e:
        print(f"Lookup error: {e}") 