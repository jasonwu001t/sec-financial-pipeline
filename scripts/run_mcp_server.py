#!/usr/bin/env python3
"""
CLI script to run the SEC Financial Data MCP Server.
Supports different transport methods and configurations.
"""

import asyncio
import argparse
import logging
import sys
import os
from pathlib import Path

# Add the project root and src directory to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from sec_mcp.server import SECFinancialMCPServer


async def run_stdio_server():
    """Run MCP server with stdio transport."""
    from mcp.server.stdio import stdio_server
    from mcp.server.models import InitializationOptions
    
    server = SECFinancialMCPServer()
    
    # Use the stdio_server context manager properly
    async with stdio_server() as streams:
        from mcp.types import ServerCapabilities
        await server.server.run(
            streams[0],  # read_stream
            streams[1],  # write_stream
            initialization_options=InitializationOptions(
                server_name="sec-financial-pipeline",
                server_version="1.0.0",
                capabilities=ServerCapabilities(tools={}, resources={})
            )
        )


async def run_sse_server(host: str = "localhost", port: int = 8001):
    """Run MCP server with SSE transport."""
    try:
        from mcp.server.sse import sse_server
        
        server = SECFinancialMCPServer()
        
        print(f"SEC Financial MCP Server starting on {host}:{port}")
        print(f"Server endpoints:")
        print(f"  SSE: http://{host}:{port}/sse")
        print(f"  Messages: http://{host}:{port}/messages")
        
        async with sse_server(host, port) as streams:
            from mcp.server.models import InitializationOptions
            from mcp.types import ServerCapabilities
            await server.server.run(
                streams[0],  # read_stream
                streams[1],  # write_stream
                initialization_options=InitializationOptions(
                    server_name="sec-financial-pipeline",
                    server_version="1.0.0",
                    capabilities=ServerCapabilities(tools={}, resources={})
                )
            )
        
    except ImportError:
        print("SSE transport not available. Install with: pip install mcp[sse]")
        sys.exit(1)


async def run_websocket_server(host: str = "localhost", port: int = 8002):
    """Run MCP server with WebSocket transport."""
    try:
        from mcp.server.websocket import websocket_server
        
        server = SECFinancialMCPServer()
        
        print(f"SEC Financial MCP Server starting on ws://{host}:{port}")
        
        async with websocket_server(host, port) as streams:
            from mcp.server.models import InitializationOptions
            from mcp.types import ServerCapabilities
            await server.server.run(
                streams[0],  # read_stream
                streams[1],  # write_stream
                initialization_options=InitializationOptions(
                    server_name="sec-financial-pipeline",
                    server_version="1.0.0",
                    capabilities=ServerCapabilities(tools={}, resources={})
                )
            )
        
    except ImportError:
        print("WebSocket transport not available. Install with: pip install mcp[websocket]")
        sys.exit(1)


def setup_logging(level: str = "INFO"):
    """Setup logging configuration."""
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {level}')
    
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stderr),
            logging.FileHandler('logs/mcp_server.log')
        ]
    )


def main():
    """Main entry point."""
    # Ensure we're in the project root directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    os.chdir(project_root)
    
    parser = argparse.ArgumentParser(
        description="SEC Financial Data MCP Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with stdio transport (default for MCP clients)
  python scripts/run_mcp_server.py

  # Run with SSE transport
  python scripts/run_mcp_server.py --transport sse --host 0.0.0.0 --port 8001

  # Run with WebSocket transport
  python scripts/run_mcp_server.py --transport websocket --port 8002

  # Run with debug logging
  python scripts/run_mcp_server.py --log-level DEBUG
        """
    )
    
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "websocket"],
        default="stdio",
        help="Transport method to use (default: stdio)"
    )
    
    parser.add_argument(
        "--host",
        default="localhost",
        help="Host to bind to (default: localhost)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=8001,
        help="Port to bind to (default: 8001 for SSE, 8002 for WebSocket)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    os.makedirs("logs", exist_ok=True)
    setup_logging(args.log_level)
    
    # Run the appropriate server
    try:
        if args.transport == "stdio":
            asyncio.run(run_stdio_server())
        elif args.transport == "sse":
            asyncio.run(run_sse_server(args.host, args.port))
        elif args.transport == "websocket":
            if args.port == 8001:  # Default port adjustment for WebSocket
                args.port = 8002
            asyncio.run(run_websocket_server(args.host, args.port))
    except KeyboardInterrupt:
        print("\nShutting down MCP server...")
    except Exception as e:
        logging.error(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 