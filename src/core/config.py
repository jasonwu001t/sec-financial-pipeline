"""
Configuration management for SEC Financial Data Pipeline.
Loads and validates settings from YAML configuration files.
"""

import os
import yaml
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import logging


@dataclass
class SECAPIConfig:
    """SEC API configuration settings."""
    base_url: str
    tickers_url: str
    user_agent: str
    timeout: int
    retry_attempts: int
    retry_delay: int
    rate_limit: Dict[str, int] = field(default_factory=dict)


@dataclass
class DataStorageConfig:
    """Data storage configuration settings."""
    base_path: str
    company_facts_path: str
    parquet_compression: str
    partition_strategy: str
    file_naming: Dict[str, str] = field(default_factory=dict)


@dataclass
class ETLConfig:
    """ETL pipeline configuration settings."""
    default_tickers_source: str
    batch_size: int
    incremental_check: bool
    skip_unchanged: bool
    max_concurrent_downloads: int
    data_retention_years: int
    schedule: Dict[str, str] = field(default_factory=dict)


@dataclass
class APIConfig:
    """FastAPI configuration settings."""
    host: str
    port: int
    reload: bool
    workers: int
    title: str
    description: str
    version: str


@dataclass
class CacheConfig:
    """Caching configuration settings."""
    type: str
    ttl: int
    max_size: int


@dataclass
class LoggingConfig:
    """Logging configuration settings."""
    level: str
    format: str
    file: str
    max_size: str
    backup_count: int


@dataclass
class PerformanceConfig:
    """Performance configuration settings."""
    max_response_size_mb: int
    query_timeout: int
    enable_compression: bool
    cors_origins: List[str] = field(default_factory=list)


@dataclass
class Config:
    """Main configuration class containing all settings."""
    sec_api: SECAPIConfig
    data_storage: DataStorageConfig
    etl: ETLConfig
    api: APIConfig
    cache: CacheConfig
    logging: LoggingConfig
    performance: PerformanceConfig
    sp500_tickers: List[str] = field(default_factory=list)


class ConfigManager:
    """Manages configuration loading and validation."""
    
    def __init__(self, config_dir: str = "./config"):
        self.config_dir = Path(config_dir)
        self._config: Optional[Config] = None
        
    def load_config(self) -> Config:
        """Load configuration from YAML files."""
        if self._config is not None:
            return self._config
            
        # Load main configuration
        config_file = self.config_dir / "config.yaml"
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
            
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
            
        # Load S&P 500 tickers
        sp500_file = self.config_dir / "sp500_tickers.json"
        sp500_tickers = []
        if sp500_file.exists():
            with open(sp500_file, 'r') as f:
                sp500_data = json.load(f)
                sp500_tickers = sp500_data.get("sp500_tickers", {}).get("tickers", [])
        
        # Create configuration objects
        self._config = Config(
            sec_api=SECAPIConfig(**config_data["sec_api"]),
            data_storage=DataStorageConfig(**config_data["data_storage"]),
            etl=ETLConfig(**config_data["etl"]),
            api=APIConfig(**config_data["api"]),
            cache=CacheConfig(**config_data["cache"]),
            logging=LoggingConfig(**config_data["logging"]),
            performance=PerformanceConfig(**config_data["performance"]),
            sp500_tickers=sp500_tickers
        )
        
        return self._config
    
    def get_config(self) -> Config:
        """Get the current configuration, loading it if necessary."""
        if self._config is None:
            return self.load_config()
        return self._config
    
    def validate_config(self) -> bool:
        """Validate the configuration settings."""
        config = self.get_config()
        
        # Validate data storage paths
        storage_path = Path(config.data_storage.base_path)
        if not storage_path.exists():
            storage_path.mkdir(parents=True, exist_ok=True)
            
        company_facts_path = Path(config.data_storage.company_facts_path)
        if not company_facts_path.exists():
            company_facts_path.mkdir(parents=True, exist_ok=True)
            
        # Validate logging directory
        log_file = Path(config.logging.file)
        log_dir = log_file.parent
        if not log_dir.exists():
            log_dir.mkdir(parents=True, exist_ok=True)
            
        return True
    
    def get_ticker_list(self, source: str = None) -> List[str]:
        """Get list of tickers based on source configuration."""
        config = self.get_config()
        
        if source is None:
            source = config.etl.default_tickers_source
            
        if source == "sp500":
            return config.sp500_tickers
        elif source == "all":
            # Return all available tickers (could be extended)
            return config.sp500_tickers
        elif source == "custom":
            # Return custom ticker list (could be loaded from another file)
            return config.sp500_tickers
        else:
            raise ValueError(f"Unknown ticker source: {source}")


# Global configuration manager instance
config_manager = ConfigManager()


def get_config() -> Config:
    """Get the global configuration instance."""
    return config_manager.get_config()


def get_ticker_list(source: str = None) -> List[str]:
    """Get ticker list from global configuration."""
    return config_manager.get_ticker_list(source)


def setup_logging():
    """Setup logging based on configuration."""
    config = get_config()
    
    # Create logs directory if it doesn't exist
    log_file = Path(config.logging.file)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.logging.level.upper()),
        format=config.logging.format,
        handlers=[
            logging.FileHandler(config.logging.file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)


if __name__ == "__main__":
    # Test configuration loading
    config = get_config()
    print(f"Loaded configuration with {len(config.sp500_tickers)} S&P 500 tickers")
    print(f"API will run on {config.api.host}:{config.api.port}")
    print(f"Data storage: {config.data_storage.company_facts_path}") 