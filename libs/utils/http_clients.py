"""
Centralized HTTP client factory for connection pooling and reuse.

This module provides cached HTTP clients to prevent SNAT port exhaustion
in Azure Functions by reusing connections across invocations.
"""
from functools import lru_cache
import httpx
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@lru_cache(maxsize=1)
def get_httpx_client() -> httpx.Client:
    """Get a cached synchronous httpx.Client with connection pooling."""
    return httpx.Client(
        timeout=httpx.Timeout(30.0),
        limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
    )


@lru_cache(maxsize=1)
def get_httpx_async_client() -> httpx.AsyncClient:
    """Get a cached asynchronous httpx.AsyncClient with connection pooling."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(30.0),
        limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
    )


@lru_cache(maxsize=1)
def get_requests_session() -> requests.Session:
    """Get a cached requests.Session with connection pooling."""
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "PUT", "POST", "DELETE", "OPTIONS"],
    )

    # Configure adapter with connection pool limits
    adapter = HTTPAdapter(
        pool_connections=10,
        pool_maxsize=20,
        max_retries=retry_strategy,
        pool_block=False,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session
