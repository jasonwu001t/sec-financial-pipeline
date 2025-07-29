"""
Individual Hedge Fund Manager Handlers
Provides specialized handlers for famous hedge fund managers.
"""

# Import from the managers module in the parent directory  
import sys
from pathlib import Path
parent_dir = Path(__file__).parent.parent
sys.path.append(str(parent_dir))

# Direct import from file to avoid module confusion
import importlib.util
managers_file = parent_dir / "managers.py"
spec = importlib.util.spec_from_file_location("managers_data", managers_file)
managers_data = importlib.util.module_from_spec(spec)
spec.loader.exec_module(managers_data)

get_famous_hedge_fund_managers = managers_data.get_famous_hedge_fund_managers
FAMOUS_MANAGERS = managers_data.FAMOUS_MANAGERS
from .berkshire_hathaway import BerkshireHathawayHandler, get_buffett_latest_portfolio, analyze_buffett_strategy

__all__ = [
    'BerkshireHathawayHandler',
    'get_buffett_latest_portfolio', 
    'analyze_buffett_strategy',
    'get_famous_hedge_fund_managers',
    'FAMOUS_MANAGERS'
] 