"""
Entry point for the DeFi Compliance Scanner.

Run with: python run.py
Or with: uvicorn app.main:app --reload
"""

import uvicorn
from app.config import HOST, PORT

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=HOST,
        port=PORT,
        reload=True,
        log_level="info",
    )
