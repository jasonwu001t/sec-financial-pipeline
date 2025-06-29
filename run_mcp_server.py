#!/usr/bin/env python3
"""
Simple wrapper script to run the SEC Financial Data MCP Server from project root.
"""

import os
import sys
import subprocess
from pathlib import Path

def main():
    # Get the project root directory (where this script is located)
    project_root = Path(__file__).parent.absolute()
    
    # Change to project root directory
    os.chdir(project_root)
    
    # Add project root and src to Python path
    env = os.environ.copy()
    python_path = str(project_root)
    if 'PYTHONPATH' in env:
        python_path = f"{python_path}:{env['PYTHONPATH']}"
    env['PYTHONPATH'] = python_path
    
    # Run the MCP server script
    script_path = project_root / "scripts" / "run_mcp_server.py"
    
    try:
        # Pass all arguments to the actual script
        cmd = [sys.executable, str(script_path)] + sys.argv[1:]
        subprocess.run(cmd, env=env, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running MCP server: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nShutting down MCP server...")
        sys.exit(0)

if __name__ == "__main__":
    main() 