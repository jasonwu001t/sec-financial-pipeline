"""
Cache Manager for SEC Financial Data API.
Provides in-memory caching with TTL support for improved performance.
"""

import asyncio
import json
import logging
from typing import Any, Dict, Optional, List
from datetime import datetime, timedelta
import hashlib
import pickle
import sys

from core.config import get_config
from core.models import CacheEntry


class CacheManager:
    """In-memory cache manager with TTL support."""
    
    def __init__(self):
        self.config = get_config()
        self.logger = logging.getLogger(__name__)
        
        # Cache storage
        self._cache: Dict[str, CacheEntry] = {}
        self._access_times: Dict[str, datetime] = {}
        
        # Cache settings
        self.ttl_seconds = self.config.cache.ttl
        self.max_size = self.config.cache.max_size
        
        # Background cleanup task
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def initialize(self):
        """Initialize the cache manager."""
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self.logger.info(f"Cache manager initialized with TTL={self.ttl_seconds}s, max_size={self.max_size}")
    
    async def close(self):
        """Close the cache manager."""
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        self.logger.info("Cache manager closed")
    
    def _generate_key(self, prefix: str, **kwargs) -> str:
        """Generate a cache key from prefix and parameters."""
        # Sort kwargs for consistent key generation
        sorted_kwargs = sorted(kwargs.items())
        key_data = f"{prefix}:" + ":".join(f"{k}={v}" for k, v in sorted_kwargs)
        
        # Hash long keys to keep them manageable
        if len(key_data) > 200:
            key_hash = hashlib.md5(key_data.encode()).hexdigest()
            return f"{prefix}:hash:{key_hash}"
        
        return key_data
    
    async def get(self, key: str) -> Optional[Any]:
        """Get a value from cache."""
        try:
            if key not in self._cache:
                return None
            
            entry = self._cache[key]
            
            # Check if expired
            if datetime.utcnow() > entry.expires_at:
                await self._remove_key(key)
                return None
            
            # Update access time
            self._access_times[key] = datetime.utcnow()
            
            return entry.data
            
        except Exception as e:
            self.logger.warning(f"Cache get error for key {key}: {e}")
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set a value in cache."""
        try:
            if ttl is None:
                ttl = self.ttl_seconds
            
            # Calculate expiration time
            expires_at = datetime.utcnow() + timedelta(seconds=ttl)
            
            # Calculate size (approximate)
            try:
                size_bytes = len(pickle.dumps(value))
            except:
                size_bytes = sys.getsizeof(value)
            
            # Check if we need to make room
            if len(self._cache) >= self.max_size:
                await self._evict_lru()
            
            # Store the entry
            entry = CacheEntry(
                key=key,
                data=value,
                expires_at=expires_at,
                size_bytes=size_bytes
            )
            
            self._cache[key] = entry
            self._access_times[key] = datetime.utcnow()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete a key from cache."""
        try:
            await self._remove_key(key)
            return True
        except Exception as e:
            self.logger.warning(f"Cache delete error for key {key}: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """Check if a key exists in cache."""
        if key not in self._cache:
            return False
        
        entry = self._cache[key]
        if datetime.utcnow() > entry.expires_at:
            await self._remove_key(key)
            return False
        
        return True
    
    async def clear_all(self) -> int:
        """Clear all cache entries."""
        count = len(self._cache)
        self._cache.clear()
        self._access_times.clear()
        return count
    
    async def _remove_key(self, key: str):
        """Remove a key from cache."""
        self._cache.pop(key, None)
        self._access_times.pop(key, None)
    
    async def _evict_lru(self):
        """Evict least recently used entries."""
        if not self._access_times:
            return
        
        # Find the least recently used key
        lru_key = min(self._access_times, key=self._access_times.get)
        await self._remove_key(lru_key)
    
    async def _cleanup_loop(self):
        """Background task to clean up expired entries."""
        while self._running:
            try:
                await self._cleanup_expired()
                await asyncio.sleep(60)  # Run cleanup every minute
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Cache cleanup error: {e}")
                await asyncio.sleep(60)
    
    async def _cleanup_expired(self):
        """Remove expired entries from cache."""
        now = datetime.utcnow()
        expired_keys = []
        
        for key, entry in self._cache.items():
            if now > entry.expires_at:
                expired_keys.append(key)
        
        for key in expired_keys:
            await self._remove_key(key)
        
        if expired_keys:
            self.logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_size = sum(entry.size_bytes or 0 for entry in self._cache.values())
        
        return {
            "total_entries": len(self._cache),
            "max_size": self.max_size,
            "ttl_seconds": self.ttl_seconds,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "cache_type": "in_memory"
        }
    
    # Convenience methods for common cache patterns
    
    async def get_company_data(self, ticker: str, years: int = 5) -> Optional[Any]:
        """Get cached company data."""
        key = self._generate_key("company_data", ticker=ticker, years=years)
        return await self.get(key)
    
    async def set_company_data(self, ticker: str, data: Any, years: int = 5, ttl: Optional[int] = None) -> bool:
        """Cache company data."""
        key = self._generate_key("company_data", ticker=ticker, years=years)
        return await self.set(key, data, ttl)
    
    async def get_metric_data(self, ticker: str, metric: str, period: str, years: int) -> Optional[Any]:
        """Get cached metric data."""
        key = self._generate_key("metric_data", ticker=ticker, metric=metric, period=period, years=years)
        return await self.get(key)
    
    async def set_metric_data(self, ticker: str, metric: str, period: str, years: int, data: Any, ttl: Optional[int] = None) -> bool:
        """Cache metric data."""
        key = self._generate_key("metric_data", ticker=ticker, metric=metric, period=period, years=years)
        return await self.set(key, data, ttl)
    
    async def get_comparison_data(self, tickers: List[str], metric: str, period: str, years: int) -> Optional[Any]:
        """Get cached comparison data."""
        ticker_key = ",".join(sorted(tickers))
        key = self._generate_key("comparison", tickers=ticker_key, metric=metric, period=period, years=years)
        return await self.get(key)
    
    async def set_comparison_data(self, tickers: List[str], metric: str, period: str, years: int, data: Any, ttl: Optional[int] = None) -> bool:
        """Cache comparison data."""
        ticker_key = ",".join(sorted(tickers))
        key = self._generate_key("comparison", tickers=ticker_key, metric=metric, period=period, years=years)
        return await self.set(key, data, ttl)
    
    async def invalidate_ticker(self, ticker: str):
        """Invalidate all cache entries for a specific ticker."""
        keys_to_remove = []
        
        for key in self._cache.keys():
            if f"ticker={ticker}" in key or f"ticker={ticker.upper()}" in key:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            await self._remove_key(key)
        
        if keys_to_remove:
            self.logger.info(f"Invalidated {len(keys_to_remove)} cache entries for ticker {ticker}")


# Singleton instance
_cache_manager: Optional[CacheManager] = None


def get_cache_manager() -> CacheManager:
    """Get the global cache manager instance."""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = CacheManager()
    return _cache_manager


# Cache decorators

def cache_result(ttl: Optional[int] = None, key_prefix: str = "func"):
    """Decorator to cache function results."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            cache_manager = get_cache_manager()
            
            # Generate cache key
            key_parts = [str(arg) for arg in args] + [f"{k}={v}" for k, v in kwargs.items()]
            key = cache_manager._generate_key(key_prefix, func_name=func.__name__, args=":".join(key_parts))
            
            # Try to get from cache
            cached_result = await cache_manager.get(key)
            if cached_result is not None:
                return cached_result
            
            # Call function and cache result
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            await cache_manager.set(key, result, ttl)
            
            return result
        
        return wrapper
    return decorator


if __name__ == "__main__":
    # Test the cache manager
    async def test_cache():
        cache = CacheManager()
        await cache.initialize()
        
        # Test basic operations
        await cache.set("test_key", {"data": "test_value"})
        result = await cache.get("test_key")
        print(f"Cached result: {result}")
        
        # Test stats
        stats = cache.get_stats()
        print(f"Cache stats: {stats}")
        
        await cache.close()
    
    asyncio.run(test_cache()) 