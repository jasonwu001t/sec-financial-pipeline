# SEC Financial Data Pipeline

A production-ready data pipeline and API service for SEC financial data that incrementally ingests company financial statements from the SEC EDGAR database, stores them in optimized Parquet files, and serves the data through a comprehensive FastAPI service.

## 🚀 Features

### ETL Pipeline

- **Incremental Data Ingestion**: Automatically fetches only new/updated SEC filings
- **S&P 500 Coverage**: Pre-configured for all S&P 500 companies
- **On-Demand Fetching**: Fetch any company data when requested via API
- **Smart Caching**: Avoids re-processing unchanged data
- **Rate Limiting**: SEC-compliant request throttling (10 req/sec)
- **Concurrent Processing**: Configurable parallel downloads
- **Robust Error Handling**: Retry logic and comprehensive logging

### Data Storage

- **Parquet Format**: Efficient columnar storage with compression
- **Organized Partitioning**: `data/company_facts/{ticker}/{annual|quarterly}/`
- **Metadata Tracking**: Data freshness and file information
- **Incremental Updates**: Only processes changed data

### FastAPI Service

- **RESTful API**: Full OpenAPI/Swagger documentation
- **Multiple Formats**: JSON, CSV, and Parquet responses
- **Performance Optimized**: In-memory caching with TTL
- **Company Comparisons**: Multi-company metric analysis
- **Health Monitoring**: System status and data freshness tracking

## 📁 Project Structure

```
sec-financial-pipeline/
├── config/
│   ├── config.yaml              # Main configuration
│   └── sp500_tickers.json       # S&P 500 company list
├── src/
│   ├── core/                    # Core utilities
│   │   ├── config.py           # Configuration management
│   │   ├── models.py           # Pydantic data models
│   │   └── sec_client.py       # SEC API client
│   ├── etl/                     # ETL pipeline
│   │   ├── data_manager.py     # Parquet file operations
│   │   └── pipeline.py         # ETL orchestration
│   └── api/                     # FastAPI service
│       ├── main.py             # API application
│       ├── cache.py            # Caching layer
│       └── data_service.py     # Business logic
├── scripts/
│   └── run_etl.py              # Command-line ETL runner
├── data/                        # Data storage (created automatically)
├── logs/                        # Application logs
├── requirements.txt             # Python dependencies
├── Dockerfile                   # Container deployment
└── README.md                    # This file
```

## 🛠️ Installation

### Prerequisites

- Python 3.11+
- 4GB+ available storage for S&P 500 data
- Internet connection for SEC API access

### Local Setup

1. **Clone the repository**:

   ```bash
   git clone <repository-url>
   cd sec-financial-pipeline
   ```

2. **Create virtual environment**:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

4. **Initialize data directories**:
   ```bash
   mkdir -p data/company_facts logs
   ```

### Docker Setup

1. **Build the container**:

   ```bash
   docker build -t sec-pipeline .
   ```

2. **Run the container**:
   ```bash
   docker run -p 8000:8000 -v $(pwd)/data:/app/data sec-pipeline
   ```

## 🚀 Quick Start

### 1. Fetch Sample Data

```bash
# Fetch data for a few companies
python scripts/run_etl.py fetch AAPL
python scripts/run_etl.py fetch MSFT
python scripts/run_etl.py fetch GOOGL
```

### 2. Start the API Server

```bash
# Development server
uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

# Production server
gunicorn src.api.main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

### 3. Access the API

- **API Documentation**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/status
- **Get AAPL Data**: http://localhost:8000/financials/AAPL
- **Get Revenue**: http://localhost:8000/financials/AAPL/revenue

## 📊 ETL Operations

### Command-Line Interface

The ETL pipeline includes a comprehensive CLI for data management:

```bash
# Show all available commands
python scripts/run_etl.py --help

# Run incremental ETL for all configured tickers
python scripts/run_etl.py incremental

# Run incremental ETL for specific tickers
python scripts/run_etl.py incremental --tickers "AAPL,MSFT,GOOGL"

# Fetch specific ticker on-demand
python scripts/run_etl.py fetch NVDA

# Run daily ETL process
python scripts/run_etl.py daily

# Show pipeline status and statistics
python scripts/run_etl.py status

# List all available tickers
python scripts/run_etl.py list-tickers

# Get detailed info about a ticker
python scripts/run_etl.py info AAPL

# Full data refresh (deletes and re-fetches)
python scripts/run_etl.py full-refresh --tickers "AAPL,MSFT"
```

### Scheduling

For production deployments, schedule the daily ETL using cron:

```bash
# Add to crontab (run daily at 6 AM)
0 6 * * * cd /path/to/sec-pipeline && python scripts/run_etl.py daily
```

## 🔌 API Endpoints

### Financial Data

| Endpoint                        | Method | Description            | Parameters                           |
| ------------------------------- | ------ | ---------------------- | ------------------------------------ |
| `/financials/{ticker}`          | GET    | Get all financial data | `period`, `years`, `format`          |
| `/financials/{ticker}/{metric}` | GET    | Get specific metric    | `period`, `years`, `format`          |
| `/financials/compare`           | POST   | Compare companies      | Request body with tickers and metric |

### Data Management

| Endpoint                     | Method | Description            |
| ---------------------------- | ------ | ---------------------- |
| `/data/tickers`              | GET    | List available tickers |
| `/data/ticker/{ticker}/info` | GET    | Get ticker information |
| `/storage/stats`             | GET    | Storage statistics     |

### System

| Endpoint              | Method | Description             |
| --------------------- | ------ | ----------------------- |
| `/status`             | GET    | System health check     |
| `/etl/status`         | GET    | ETL pipeline status     |
| `/etl/fetch/{ticker}` | POST   | Trigger on-demand fetch |
| `/cache/stats`        | GET    | Cache statistics        |

### Example API Calls

```bash
# Get Apple's financial data (last 5 years, annual)
curl "http://localhost:8000/financials/AAPL?period=annual&years=5"

# Get Microsoft's revenue data as CSV
curl "http://localhost:8000/financials/MSFT/revenue?format=csv" > msft_revenue.csv

# Compare revenue across tech companies
curl -X POST "http://localhost:8000/financials/compare" \
  -H "Content-Type: application/json" \
  -d '{
    "tickers": ["AAPL", "MSFT", "GOOGL", "NVDA"],
    "metric": "revenue",
    "period": "annual",
    "years": 3
  }'

# Check system status
curl "http://localhost:8000/status"

# Trigger on-demand fetch for a new ticker
curl -X POST "http://localhost:8000/etl/fetch/TSLA"
```

## 📈 Available Financial Metrics

The pipeline supports comprehensive financial metrics:

- **Revenue**: Total company revenue
- **Net Income**: Net profit/loss
- **Total Assets**: Balance sheet total assets
- **Total Liabilities**: Total liabilities
- **Cash**: Cash and cash equivalents
- **Shareholders Equity**: Total equity
- **Earnings Per Share**: Basic and diluted EPS
- **Operating Income**: Income from operations
- **Gross Profit**: Revenue minus cost of goods sold
- **Research & Development**: R&D expenses
- **Debt**: Total debt obligations

## ⚙️ Configuration

### Main Configuration (`config/config.yaml`)

```yaml
sec_api:
  user_agent: "SEC-Financial-Pipeline/1.0 (Contact: your-email@domain.com)"
  rate_limit:
    requests_per_second: 10
  timeout: 30
  retry_attempts: 3

data_storage:
  base_path: "./data"
  parquet_compression: "snappy"

etl:
  default_tickers_source: "sp500"
  batch_size: 10
  max_concurrent_downloads: 3
  skip_unchanged: true

api:
  host: "0.0.0.0"
  port: 8000
  title: "SEC Financial Data API"

cache:
  ttl: 3600 # 1 hour
  max_size: 1000

performance:
  enable_compression: true
  cors_origins: ["*"]
```

### Environment Variables

```bash
# Override configuration with environment variables
export SEC_API_USER_AGENT="YourApp/1.0 (Contact: your-email@domain.com)"
export DATA_STORAGE_BASE_PATH="/custom/data/path"
export API_HOST="127.0.0.1"
export API_PORT="8080"
```

## 📊 Data Format

### Parquet File Structure

Each company's data is stored in partitioned Parquet files:

```
data/company_facts/
├── AAPL/
│   ├── annual/
│   │   ├── AAPL_2023_annual.parquet
│   │   ├── AAPL_2022_annual.parquet
│   │   └── ...
│   └── quarterly/
│       ├── AAPL_2023_Q4.parquet
│       ├── AAPL_2023_Q3.parquet
│       └── ...
├── MSFT/
└── ...
```

### Data Schema

Each Parquet file contains:

| Column          | Type   | Description                             |
| --------------- | ------ | --------------------------------------- |
| `label`         | string | XBRL concept label                      |
| `description`   | string | Human-readable description              |
| `value`         | float  | Numerical value                         |
| `unit`          | string | Unit of measurement (USD, shares, etc.) |
| `fiscal_year`   | int    | Fiscal year                             |
| `fiscal_period` | string | Fiscal period (FY, Q1, Q2, Q3, Q4)      |
| `start_date`    | date   | Period start date                       |
| `end_date`      | date   | Period end date                         |
| `instant_date`  | date   | Point-in-time date                      |
| `form`          | string | SEC form type (10-K, 10-Q, 8-K)         |

## 🔍 Monitoring & Logging

### Log Files

- **Application logs**: `logs/app.log`
- **ETL logs**: `logs/etl.log`
- **API logs**: `logs/api.log`

### Health Monitoring

Monitor system health via the `/status` endpoint:

```json
{
  "status": "healthy",
  "api_version": "1.0.0",
  "data_freshness": {
    "available_tickers": 485,
    "total_files": 2430,
    "total_size_mb": 1250.5,
    "last_etl_run": "2024-01-09T06:00:00Z",
    "etl_success_rate": 98.5
  },
  "cache_stats": {
    "total_entries": 150,
    "total_size_mb": 45.2
  }
}
```

## 🧪 Testing

### Unit Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test module
pytest tests/test_etl.py
```

### Integration Tests

```bash
# Test ETL pipeline
python scripts/run_etl.py fetch AAPL --debug

# Test API endpoints
curl "http://localhost:8000/status"
curl "http://localhost:8000/financials/AAPL"
```

## 🚀 Production Deployment

### Docker Compose

```yaml
version: "3.8"
services:
  sec-pipeline:
    build: .
    ports:
      - "8000:8000"
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    environment:
      - SEC_API_USER_AGENT=YourApp/1.0 (Contact: your-email@domain.com)
    restart: unless-stopped

  # Optional: Add nginx for load balancing
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
    depends_on:
      - sec-pipeline
```

### Production Checklist

- [ ] Configure appropriate `user_agent` in config
- [ ] Set up persistent data volume
- [ ] Configure log rotation
- [ ] Set up monitoring/alerting
- [ ] Schedule daily ETL via cron
- [ ] Configure firewall rules
- [ ] Set up SSL/TLS termination
- [ ] Monitor disk space usage

## 📋 SEC Compliance

This pipeline follows SEC guidelines:

- **Rate Limiting**: Maximum 10 requests per second
- **User Agent**: Identifies the application and contact information
- **Respectful Usage**: Only fetches data when needed
- **Data Attribution**: Maintains source attribution in all data

### Required User Agent Format

Update the `user_agent` in `config/config.yaml`:

```yaml
sec_api:
  user_agent: "YourCompany/1.0 (Contact: your-email@domain.com) - SEC Financial Data Pipeline"
```

## 🔧 Troubleshooting

### Common Issues

1. **Rate Limiting Errors**:

   - Check that `requests_per_second` is ≤ 10
   - Verify user agent is properly configured

2. **Missing Data**:

   - Some companies may not have all metrics
   - Check SEC filings directly on EDGAR

3. **Storage Issues**:

   - Monitor disk space (S&P 500 data ~2-5GB)
   - Check file permissions on data directory

4. **API Performance**:
   - Increase cache TTL for slower-changing data
   - Use pagination for large datasets

### Debug Mode

Enable debug logging:

```bash
python scripts/run_etl.py --debug incremental
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- SEC for providing free access to financial data
- FastAPI and Pydantic communities
- Apache Arrow/Parquet for efficient data storage

---

**📧 Support**: For questions or issues, please open a GitHub issue or contact the maintainers.
