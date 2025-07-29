# 🏦 SEC 13F Hedge Fund Analysis

> **Track and analyze institutional investment manager portfolios from SEC 13F filings**

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104%2B-009688)](https://fastapi.tiangolo.com)
[![SEC 13F](https://img.shields.io/badge/SEC%2013F-Filing%20Analysis-1f77b4)](https://www.sec.gov/divisions/investment/13ffaq.htm)

## 🎯 **Overview**

The 13F module provides comprehensive analysis of institutional investment manager portfolios through SEC Form 13F filings. Track famous hedge fund managers, analyze their investment strategies, and gain insights into institutional money flows.

## ✨ **Key Features**

### 🏆 **Famous Hedge Fund Database**

- Pre-configured data for 20+ famous hedge fund managers
- Warren Buffett/Berkshire Hathaway
- Ray Dalio/Bridgewater Associates
- Ken Griffin/Citadel
- And many more...

### 📊 **Portfolio Analysis**

- Track portfolio changes over time
- Identify new positions and exits
- Analyze concentration ratios
- Sector allocation analysis
- Performance correlation studies

### 🎯 **Specialized Handlers**

- Individual manager-specific analysis
- Custom insights for different investment styles
- Buffett-specific strategy analysis tools
- Extensible framework for adding new managers

### ⚡ **Real-Time API Access**

- RESTful endpoints for all 13F data
- JSON, CSV export formats
- Health monitoring and status checks
- Integration with existing SEC pipeline

## 🚀 **Quick Start**

### **1. Get Famous Hedge Fund Managers**

```python
from sec_13f.managers import get_famous_hedge_fund_managers

# Get all famous managers
managers = get_famous_hedge_fund_managers()
print(f"Found {len(managers)} hedge fund managers")

for manager in managers[:5]:
    print(f"{manager.common_name} (CIK: {manager.cik})")
    print(f"  Personnel: {', '.join(manager.key_personnel)}")
```

### **2. Analyze Berkshire Hathaway Portfolio**

```python
import asyncio
from sec_13f.managers.berkshire_hathaway import BerkshireHathawayHandler

async def analyze_berkshire():
    async with BerkshireHathawayHandler() as handler:
        # Get latest portfolio
        portfolio = await handler.get_latest_portfolio()
        print(f"Portfolio value: ${portfolio.total_value:,.0f}K")
        print(f"Holdings: {portfolio.total_holdings_count}")

        # Get comprehensive analysis
        insights = await handler.get_berkshire_insights()
        print("Investment strategy insights generated!")

asyncio.run(analyze_berkshire())
```

### **3. Use 13F API Endpoints**

```bash
# Start the API server
python src/api/main.py

# Get all hedge fund managers
curl http://localhost:8000/hedge-funds/managers

# Get Berkshire Hathaway's latest 13F filing
curl http://localhost:8000/hedge-funds/filings/0001067983/latest

# Get Berkshire analysis
curl http://localhost:8000/hedge-funds/analysis/berkshire

# Get top holdings for any manager
curl http://localhost:8000/hedge-funds/top-holdings/0001067983?limit=10
```

## 🏗️ **Architecture**

### **Core Components**

```
src/sec_13f/
├── client.py              # SEC 13F API client
├── managers.py            # Famous hedge fund database
├── managers/              # Individual manager handlers
│   ├── berkshire_hathaway.py
│   └── __init__.py
└── __init__.py

src/core/
├── sec_data_service.py    # Centralized SEC data service
└── models.py              # 13F data models (Form13F, etc.)

src/api/
└── hedge_fund_endpoints.py # REST API endpoints
```

### **Data Models**

- **`Form13F`**: Complete 13F filing with holdings
- **`InstitutionalHolding`**: Individual security holding
- **`HedgeFundManager`**: Manager information and metadata
- **`PortfolioAnalysis`**: Portfolio analysis results

## 📋 **Available Hedge Fund Managers**

| Manager                  | CIK        | Key Personnel                  | Focus           |
| ------------------------ | ---------- | ------------------------------ | --------------- |
| Berkshire Hathaway       | 0001067983 | Warren Buffett, Charlie Munger | Value Investing |
| Bridgewater Associates   | 0001350694 | Ray Dalio                      | Global Macro    |
| Renaissance Technologies | 0001037389 | James Simons                   | Quantitative    |
| Citadel                  | 0001423053 | Ken Griffin                    | Multi-Strategy  |
| Greenlight Capital       | 0001079114 | David Einhorn                  | Value/Short     |
| Pershing Square          | 0001336528 | Bill Ackman                    | Activist        |
| Third Point              | 0001040273 | Daniel Loeb                    | Event-Driven    |
| Tiger Global             | 0001167483 | Chase Coleman                  | Technology      |
| Point72                  | 0001603466 | Steven Cohen                   | Multi-Manager   |
| D.E. Shaw                | 0000928645 | David Shaw                     | Quantitative    |
| ...and 10 more           |            |                                |                 |

## 🔧 **API Endpoints**

### **Managers**

- `GET /hedge-funds/managers` - List all managers
- `GET /hedge-funds/managers/{cik}` - Get specific manager

### **Filings**

- `GET /hedge-funds/filings/{cik}` - Get 13F filings
- `GET /hedge-funds/filings/{cik}/latest` - Get latest filing

### **Analysis**

- `GET /hedge-funds/analysis/berkshire` - Berkshire analysis
- `GET /hedge-funds/analysis/portfolio-changes/{cik}` - Portfolio changes
- `GET /hedge-funds/top-holdings/{cik}` - Top holdings

### **Utilities**

- `GET /hedge-funds/health` - Health check

## 🧪 **Testing**

### **Run Basic Tests**

```bash
python src/test_13f_functionality.py
```

### **Run Comprehensive Tests**

```bash
python src/comprehensive_13f_test.py
```

### **Test Individual Components**

```python
# Test famous managers
python -c "from sec_13f.managers import get_famous_hedge_fund_managers; print(len(get_famous_hedge_fund_managers()))"

# Test Berkshire handler
python -c "import asyncio; from sec_13f.managers.berkshire_hathaway import get_buffett_latest_portfolio; print(asyncio.run(get_buffett_latest_portfolio()))"
```

## 📊 **Use Cases**

### **Investment Research**

- Track what successful hedge funds are buying/selling
- Identify emerging investment trends
- Benchmark your portfolio against institutional investors

### **Academic Research**

- Study institutional investor behavior
- Analyze portfolio concentration and diversification
- Research factor exposures of different managers

### **Financial Media/Journalism**

- Track newsworthy portfolio changes
- Generate insights for financial reporting
- Monitor activist investor activities

### **Compliance and Risk**

- Monitor institutional ownership changes
- Track regulatory filing patterns
- Risk assessment for counterparties

## 🔮 **Future Enhancements**

- [ ] CUSIP to ticker symbol mapping service
- [ ] Sector classification and analysis
- [ ] Portfolio performance attribution
- [ ] Additional manager-specific handlers
- [ ] Real-time filing notifications
- [ ] Machine learning models for strategy prediction

## 🤝 **Contributing**

To add a new hedge fund manager:

1. Add manager data to `FAMOUS_MANAGERS` in `src/sec_13f/managers.py`
2. Create specialized handler in `src/sec_13f/managers/`
3. Add API endpoints if needed
4. Update tests and documentation

## 📄 **Related Documentation**

- [Main Pipeline README](README.md)
- [MCP Server Documentation](README_MCP.md)
- [Pipeline Documentation](README_PIPELINE.md)
- [SEC Form 13F Information](https://www.sec.gov/divisions/investment/13ffaq.htm)

## ⚖️ **Legal Notice**

This software is for educational and research purposes. Always verify data independently and comply with applicable regulations when using institutional investment data. The authors are not responsible for any investment decisions made based on this data.
