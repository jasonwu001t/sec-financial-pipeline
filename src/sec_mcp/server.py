"""
MCP Server for SEC Financial Data Pipeline.
Provides natural language interface for SEC financial data queries and analysis.
"""

import asyncio
import json
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, date
from pathlib import Path
import pandas as pd

from mcp.server import Server
from mcp.types import (
    CallToolRequest,
    CallToolResult,
    ListResourcesRequest,
    ListResourcesResult,
    ListToolsRequest,
    ListToolsResult,
    ReadResourceRequest,
    ReadResourceResult,
    Resource,
    Tool,
    TextContent,
)

from core.models import ReportingPeriod, CompanyData, FinancialFact
from etl.data_manager import DataManager
from api.data_service import DataService
from api.cache import CacheManager


class SECFinancialMCPServer:
    """MCP Server for SEC Financial Data Pipeline."""
    
    def __init__(self):
        self.server = Server("sec-financial-pipeline")
        self.logger = logging.getLogger(__name__)
        
        # Initialize components (they will load their own config)
        self.data_manager = DataManager()
        self.cache_manager = CacheManager()
        self.data_service = DataService(self.data_manager, self.cache_manager)
        
        # Register request handlers
        self._register_handlers()
    
    def _register_handlers(self):
        """Register all MCP request handlers."""
        
        # Register tool handlers
        self.server.request_handlers["tools/list"] = self._handle_list_tools
        self.server.request_handlers["tools/call"] = self._handle_call_tool
        
        # Register resource handlers  
        self.server.request_handlers["resources/list"] = self._handle_list_resources
        self.server.request_handlers["resources/read"] = self._handle_read_resource
    
    async def _handle_list_tools(self, request: ListToolsRequest) -> ListToolsResult:
        """Handle tools/list request."""
        tools = [
            Tool(
                name="get_company_financials",
                description="Get comprehensive financial data for a company",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "ticker": {"type": "string", "description": "Stock ticker symbol"},
                        "years": {"type": "integer", "default": 5, "description": "Number of years of data"},
                        "period": {"type": "string", "default": "annual", "enum": ["annual", "quarterly"]}
                    },
                    "required": ["ticker"]
                }
            ),
            Tool(
                name="get_financial_metric",
                description="Get specific financial metric for a company",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "ticker": {"type": "string", "description": "Stock ticker symbol"},
                        "metric": {"type": "string", "description": "Financial metric name"},
                        "years": {"type": "integer", "default": 5, "description": "Number of years of data"},
                        "period": {"type": "string", "default": "annual", "enum": ["annual", "quarterly"]}
                    },
                    "required": ["ticker", "metric"]
                }
            ),
            Tool(
                name="compare_companies",
                description="Compare financial metrics across multiple companies",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tickers": {"type": "array", "items": {"type": "string"}, "description": "List of ticker symbols"},
                        "metric": {"type": "string", "description": "Financial metric to compare"},
                        "years": {"type": "integer", "default": 3, "description": "Number of years of data"},
                        "period": {"type": "string", "default": "annual", "enum": ["annual", "quarterly"]}
                    },
                    "required": ["tickers", "metric"]
                }
            ),
            Tool(
                name="analyze_financial_trends",
                description="Analyze financial trends for multiple metrics over time",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "ticker": {"type": "string", "description": "Stock ticker symbol"},
                        "metrics": {"type": "array", "items": {"type": "string"}, "description": "List of metrics to analyze"},
                        "years": {"type": "integer", "default": 10, "description": "Number of years of data"}
                    },
                    "required": ["ticker", "metrics"]
                }
            ),
            Tool(
                name="generate_financial_report",
                description="Generate comprehensive financial report for a company",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "ticker": {"type": "string", "description": "Stock ticker symbol"},
                        "report_type": {"type": "string", "default": "comprehensive", "enum": ["comprehensive", "summary", "growth", "profitability"]},
                        "years": {"type": "integer", "default": 5, "description": "Number of years to include"}
                    },
                    "required": ["ticker"]
                }
            ),
            Tool(
                name="get_available_metrics",
                description="Get list of available financial metrics",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "ticker": {"type": "string", "description": "Optional ticker to get metrics for specific company"}
                    },
                    "required": []
                }
            )
        ]
        
        return ListToolsResult(tools=tools)
    
    async def _handle_call_tool(self, request: CallToolRequest) -> CallToolResult:
        """Handle tools/call request."""
        try:
            tool_name = request.params.name
            arguments = request.params.arguments or {}
            
            if tool_name == "get_company_financials":
                result = await self._get_company_financials(**arguments)
            elif tool_name == "get_financial_metric":
                result = await self._get_financial_metric(**arguments)
            elif tool_name == "compare_companies":
                result = await self._compare_companies(**arguments)
            elif tool_name == "analyze_financial_trends":
                result = await self._analyze_financial_trends(**arguments)
            elif tool_name == "generate_financial_report":
                result = await self._generate_financial_report(**arguments)
            elif tool_name == "get_available_metrics":
                result = await self._get_available_metrics(**arguments)
            else:
                return CallToolResult(
                    content=[TextContent(type="text", text=f"Unknown tool: {tool_name}")]
                )
            
            # Convert result to text content
            result_text = json.dumps(result, indent=2, default=str)
            
            return CallToolResult(
                content=[TextContent(type="text", text=result_text)]
            )
            
        except Exception as e:
            self.logger.error(f"Error calling tool {tool_name}: {e}")
            return CallToolResult(
                content=[TextContent(type="text", text=f"Error: {str(e)}")]
            )
    
    async def _handle_list_resources(self, request: ListResourcesRequest) -> ListResourcesResult:
        """Handle resources/list request."""
        resources = [
            Resource(
                uri="sec://companies/sp500",
                name="S&P 500 Companies",
                description="List of S&P 500 companies with tickers and basic info",
                mimeType="application/json"
            ),
            Resource(
                uri="sec://metrics/available",
                name="Available Financial Metrics",
                description="List of all available financial metrics",
                mimeType="application/json"
            )
        ]
        
        return ListResourcesResult(resources=resources)
    
    async def _handle_read_resource(self, request: ReadResourceRequest) -> ReadResourceResult:
        """Handle resources/read request."""
        uri = request.params.uri
        
        try:
            if uri == "sec://companies/sp500":
                # Load S&P 500 companies list
                from core.config import get_config
                config = get_config()
                
                # Try to load from config directory
                sp500_file = Path("config/sp500_tickers.json")
                if sp500_file.exists():
                    with open(sp500_file, 'r') as f:
                        sp500_data = json.load(f)
                    content = json.dumps(sp500_data, indent=2)
                else:
                    # Return the tickers from config
                    content = json.dumps({"sp500_tickers": config.sp500_tickers}, indent=2)
                    
            elif uri == "sec://metrics/available":
                metrics = self.data_service.get_available_metrics()
                content = json.dumps(metrics, indent=2)
            else:
                return ReadResourceResult(
                    contents=[TextContent(type="text", text=f"Unknown resource: {uri}")]
                )
            
            return ReadResourceResult(
                contents=[TextContent(type="text", text=content)]
            )
            
        except Exception as e:
            self.logger.error(f"Error reading resource {uri}: {e}")
            return ReadResourceResult(
                contents=[TextContent(type="text", text=f"Error reading resource: {str(e)}")]
            )
    
    # Tool implementation methods
    async def _get_company_financials(
        self,
        ticker: str,
        years: int = 5,
        period: str = "annual"
    ) -> List[Dict[str, Any]]:
        """Get comprehensive financial data for a company."""
        try:
            ticker = ticker.upper()
            period_enum = ReportingPeriod.ANNUAL if period.lower() == "annual" else ReportingPeriod.QUARTERLY
            
            # Get company data
            company_data = await self.data_service.get_company_data(ticker, years)
            if not company_data:
                return [{"error": f"No financial data found for ticker {ticker}"}]
            
            # Format response
            result = {
                "company_info": {
                    "ticker": company_data.company_info.ticker,
                    "name": company_data.company_info.name,
                    "cik": company_data.company_info.cik,
                    "industry": company_data.company_info.industry,
                    "sector": company_data.company_info.sector
                },
                "last_updated": company_data.last_updated.isoformat(),
                "financial_statements": []
            }
            
            # Get key metrics for each year
            key_metrics = ["revenue", "net_income", "total_assets", "shareholders_equity", "cash"]
            
            for metric in key_metrics:
                metric_data = await self.data_service.get_metric_data(ticker, metric, period_enum, years)
                if metric_data:
                    result["financial_statements"].append({
                        "metric": metric,
                        "data": metric_data
                    })
            
            return [result]
            
        except Exception as e:
            self.logger.error(f"Error getting company financials for {ticker}: {e}")
            return [{"error": f"Failed to retrieve financial data: {str(e)}"}]
    
    async def _get_financial_metric(
        self,
        ticker: str,
        metric: str,
        years: int = 5,
        period: str = "annual"
    ) -> List[Dict[str, Any]]:
        """Get specific financial metric for a company."""
        try:
            ticker = ticker.upper()
            metric = metric.lower()
            period_enum = ReportingPeriod.ANNUAL if period.lower() == "annual" else ReportingPeriod.QUARTERLY
            
            # Get metric data
            metric_data = await self.data_service.get_metric_data(ticker, metric, period_enum, years)
            if not metric_data:
                return [{"error": f"No data found for {metric} for ticker {ticker}"}]
            
            # Calculate trends and insights
            values = [d.get("value", 0) for d in metric_data if d.get("value") is not None]
            
            result = {
                "ticker": ticker,
                "metric": metric,
                "period": period,
                "data": metric_data,
                "analysis": {
                    "total_periods": len(metric_data),
                    "latest_value": values[0] if values else None,
                    "oldest_value": values[-1] if len(values) > 1 else None,
                    "average_value": sum(values) / len(values) if values else None,
                    "growth_rate": ((values[0] / values[-1]) - 1) * 100 if len(values) > 1 and values[-1] != 0 else None
                }
            }
            
            return [result]
            
        except Exception as e:
            self.logger.error(f"Error getting metric {metric} for {ticker}: {e}")
            return [{"error": f"Failed to retrieve metric data: {str(e)}"}]
    
    async def _compare_companies(
        self,
        tickers: List[str],
        metric: str,
        years: int = 3,
        period: str = "annual"
    ) -> List[Dict[str, Any]]:
        """Compare financial metrics across multiple companies."""
        try:
            if len(tickers) < 2:
                return [{"error": "Need at least 2 companies to compare"}]
            if len(tickers) > 10:
                return [{"error": "Maximum 10 companies allowed for comparison"}]
            
            tickers = [t.upper() for t in tickers]
            metric = metric.lower()
            period_enum = ReportingPeriod.ANNUAL if period.lower() == "annual" else ReportingPeriod.QUARTERLY
            
            # Get comparison data
            comparison_data = await self.data_service.compare_companies(tickers, metric, period_enum, years)
            
            if not comparison_data:
                return [{"error": f"No comparison data available for metric {metric}"}]
            
            # Analyze comparison data
            latest_values = []
            for company_data in comparison_data:
                if company_data["data"]:
                    latest_value = company_data["data"][0].get("value")
                    if latest_value is not None:
                        latest_values.append({
                            "ticker": company_data["ticker"],
                            "value": latest_value,
                            "fiscal_year": company_data["data"][0].get("fiscal_year")
                        })
            
            # Sort by value (descending)
            latest_values.sort(key=lambda x: x["value"], reverse=True)
            
            result = {
                "metric": metric,
                "period": period,
                "years": years,
                "companies": comparison_data,
                "ranking": latest_values,
                "analysis": {
                    "best_performer": latest_values[0] if latest_values else None,
                    "worst_performer": latest_values[-1] if latest_values else None,
                    "average_value": sum(v["value"] for v in latest_values) / len(latest_values) if latest_values else None
                }
            }
            
            return [result]
            
        except Exception as e:
            self.logger.error(f"Error comparing companies {tickers}: {e}")
            return [{"error": f"Failed to compare companies: {str(e)}"}]
    
    async def _analyze_financial_trends(
        self,
        ticker: str,
        metrics: List[str],
        years: int = 10
    ) -> List[Dict[str, Any]]:
        """Analyze financial trends for multiple metrics over time."""
        try:
            ticker = ticker.upper()
            metrics = [m.lower() for m in metrics]
            period_enum = ReportingPeriod.ANNUAL
            
            trend_analysis = {
                "ticker": ticker,
                "metrics_analyzed": metrics,
                "years": years,
                "trends": {}
            }
            
            for metric in metrics:
                metric_data = await self.data_service.get_metric_data(ticker, metric, period_enum, years)
                
                if metric_data:
                    values = [d.get("value", 0) for d in metric_data if d.get("value") is not None]
                    years_data = [d.get("fiscal_year") for d in metric_data if d.get("fiscal_year")]
                    
                    if len(values) >= 2:
                        # Calculate compound annual growth rate (CAGR)
                        n_years = len(values) - 1
                        cagr = ((values[0] / values[-1]) ** (1/n_years) - 1) * 100 if values[-1] != 0 else None
                        
                        # Calculate year-over-year growth rates
                        yoy_growth = []
                        for i in range(len(values) - 1):
                            if values[i+1] != 0:
                                growth = ((values[i] / values[i+1]) - 1) * 100
                                yoy_growth.append({
                                    "year": years_data[i] if i < len(years_data) else None,
                                    "growth_rate": growth
                                })
                        
                        trend_analysis["trends"][metric] = {
                            "values": metric_data,
                            "cagr": cagr,
                            "yoy_growth": yoy_growth,
                            "volatility": pd.Series([g["growth_rate"] for g in yoy_growth]).std() if yoy_growth else None,
                            "trend_direction": "increasing" if cagr and cagr > 0 else "decreasing" if cagr and cagr < 0 else "stable"
                        }
            
            return [trend_analysis]
            
        except Exception as e:
            self.logger.error(f"Error analyzing trends for {ticker}: {e}")
            return [{"error": f"Failed to analyze trends: {str(e)}"}]
    
    async def _generate_financial_report(
        self,
        ticker: str,
        report_type: str = "comprehensive",
        years: int = 5
    ) -> List[Dict[str, Any]]:
        """Generate comprehensive financial report for a company."""
        try:
            ticker = ticker.upper()
            
            # Get company data
            company_data = await self.data_service.get_company_data(ticker, years)
            if not company_data:
                return [{"error": f"No data available for {ticker}"}]
            
            # Generate different types of reports
            if report_type == "comprehensive":
                return await self._generate_comprehensive_report(ticker, company_data, years)
            elif report_type == "summary":
                return await self._generate_summary_report(ticker, company_data, years)
            elif report_type == "growth":
                return await self._generate_growth_report(ticker, company_data, years)
            elif report_type == "profitability":
                return await self._generate_profitability_report(ticker, company_data, years)
            else:
                return [{"error": f"Unknown report type: {report_type}"}]
            
        except Exception as e:
            self.logger.error(f"Error generating financial report for {ticker}: {e}")
            return [{"error": f"Failed to generate report: {str(e)}"}]
    
    async def _get_available_metrics(
        self,
        ticker: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get list of available financial metrics."""
        try:
            available_metrics = self.data_service.get_available_metrics()
            
            if ticker:
                ticker = ticker.upper()
                # Get company-specific metric availability
                company_data = await self.data_service.get_company_data(ticker, 1)
                if company_data:
                    # Analyze which metrics have data for this company
                    for metric in available_metrics:
                        metric_data = await self.data_service.get_metric_data(
                            ticker, metric["name"], ReportingPeriod.ANNUAL, 1
                        )
                        metric["has_data"] = bool(metric_data)
            
            return available_metrics
            
        except Exception as e:
            self.logger.error(f"Error getting available metrics: {e}")
            return [{"error": f"Failed to get available metrics: {str(e)}"}]
    
    # Report generation helper methods
    async def _generate_comprehensive_report(self, ticker: str, company_data: CompanyData, years: int) -> List[Dict[str, Any]]:
        """Generate comprehensive financial report."""
        
        # Get key metrics
        key_metrics = ["revenue", "net_income", "total_assets", "shareholders_equity", "cash", "debt"]
        period = ReportingPeriod.ANNUAL
        
        report = {
            "report_type": "comprehensive",
            "company": {
                "ticker": ticker,
                "name": company_data.company_info.name,
                "sector": company_data.company_info.sector,
                "industry": company_data.company_info.industry
            },
            "report_date": datetime.now().isoformat(),
            "time_period": f"{years} years",
            "sections": {}
        }
        
        # Financial Performance Section
        performance_data = {}
        for metric in key_metrics:
            metric_data = await self.data_service.get_metric_data(ticker, metric, period, years)
            if metric_data:
                performance_data[metric] = metric_data
        
        report["sections"]["financial_performance"] = performance_data
        
        # Growth Analysis Section
        growth_analysis = {}
        for metric in ["revenue", "net_income"]:
            if metric in performance_data and len(performance_data[metric]) >= 2:
                values = [d.get("value", 0) for d in performance_data[metric] if d.get("value") is not None]
                if len(values) >= 2 and values[-1] != 0:
                    cagr = ((values[0] / values[-1]) ** (1/(len(values)-1)) - 1) * 100
                    growth_analysis[metric] = {
                        "cagr": cagr,
                        "trend": "positive" if cagr > 0 else "negative"
                    }
        
        report["sections"]["growth_analysis"] = growth_analysis
        
        # Financial Health Indicators
        health_indicators = {}
        if "total_assets" in performance_data and "debt" in performance_data:
            latest_assets = performance_data["total_assets"][0].get("value") if performance_data["total_assets"] else None
            latest_debt = performance_data["debt"][0].get("value") if performance_data["debt"] else None
            
            if latest_assets and latest_debt:
                debt_to_assets = (latest_debt / latest_assets) * 100
                health_indicators["debt_to_assets_ratio"] = debt_to_assets
                health_indicators["financial_leverage"] = "high" if debt_to_assets > 50 else "moderate" if debt_to_assets > 25 else "low"
        
        report["sections"]["financial_health"] = health_indicators
        
        return [report]
    
    async def _generate_summary_report(self, ticker: str, company_data: CompanyData, years: int) -> List[Dict[str, Any]]:
        """Generate summary financial report."""
        period = ReportingPeriod.ANNUAL
        
        # Get latest values for key metrics
        revenue_data = await self.data_service.get_metric_data(ticker, "revenue", period, 1)
        net_income_data = await self.data_service.get_metric_data(ticker, "net_income", period, 1)
        assets_data = await self.data_service.get_metric_data(ticker, "total_assets", period, 1)
        
        summary = {
            "report_type": "summary",
            "company": company_data.company_info.name,
            "ticker": ticker,
            "latest_metrics": {
                "revenue": revenue_data[0] if revenue_data else None,
                "net_income": net_income_data[0] if net_income_data else None,
                "total_assets": assets_data[0] if assets_data else None
            },
            "data_as_of": company_data.last_updated.isoformat()
        }
        
        return [summary]
    
    async def _generate_growth_report(self, ticker: str, company_data: CompanyData, years: int) -> List[Dict[str, Any]]:
        """Generate growth-focused financial report."""
        return await self._analyze_financial_trends(ticker, ["revenue", "net_income", "total_assets"], years)
    
    async def _generate_profitability_report(self, ticker: str, company_data: CompanyData, years: int) -> List[Dict[str, Any]]:
        """Generate profitability-focused financial report."""
        period = ReportingPeriod.ANNUAL
        
        # Get profitability metrics
        revenue_data = await self.data_service.get_metric_data(ticker, "revenue", period, years)
        net_income_data = await self.data_service.get_metric_data(ticker, "net_income", period, years)
        operating_income_data = await self.data_service.get_metric_data(ticker, "operating_income", period, years)
        
        profitability_report = {
            "report_type": "profitability",
            "company": company_data.company_info.name,
            "ticker": ticker,
            "metrics": {
                "revenue": revenue_data,
                "net_income": net_income_data,
                "operating_income": operating_income_data
            }
        }
        
        # Calculate margin ratios
        if revenue_data and net_income_data:
            margins = []
            for i in range(min(len(revenue_data), len(net_income_data))):
                rev = revenue_data[i].get("value")
                ni = net_income_data[i].get("value")
                if rev and ni and rev != 0:
                    margin = (ni / rev) * 100
                    margins.append({
                        "fiscal_year": revenue_data[i].get("fiscal_year"),
                        "net_margin": margin
                    })
            
            profitability_report["margins"] = margins
        
        return [profitability_report]
    
    async def run(self, transport):
        """Run the MCP server."""
        await self.server.run(transport)


async def main():
    """Main entry point for MCP server."""
    import sys
    from mcp.server.stdio import stdio_server
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Create and run server
    server = SECFinancialMCPServer()
    
    # Run with stdio transport
    await server.run(stdio_server())


if __name__ == "__main__":
    asyncio.run(main()) 