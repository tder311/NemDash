#!/usr/bin/env python3
"""
Run script for NEM Dashboard Backend
"""

import uvicorn
import os
from pathlib import Path

if __name__ == "__main__":
    # Load environment variables
    from dotenv import load_dotenv
    
    # Load .env file if it exists
    env_path = Path(__file__).parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
    
    # Configuration
    host = os.getenv('HOST', '0.0.0.0')
    port = int(os.getenv('PORT', '8000'))
    reload = os.getenv('RELOAD', 'True').lower() == 'true'
    log_level = os.getenv('LOG_LEVEL', 'info').lower()
    
    print(f"Starting NEM Dashboard Backend on {host}:{port}")
    print(f"Reload: {reload}, Log level: {log_level}")
    
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=reload,
        log_level=log_level
    )