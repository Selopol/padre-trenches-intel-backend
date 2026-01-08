"""
Padre Trenches Dev Intel - Backend Server
Tracks Solana token developers using Pump.fun API
"""

import os
import json
import time
import asyncio
import logging
import sqlite3
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from contextlib import asynccontextmanager

import requests
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Configuration
PUMPFUN_API_URL = "https://frontend-api-v3.pump.fun"
TWITTER_API_KEY = "new1_defb379335c44d58890c0e2c59ada78f"
DATABASE_PATH = os.getenv("DATABASE_PATH", "dev_intel.db")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database setup
def init_database():
    """Initialize SQLite database with required tables"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            mint TEXT PRIMARY KEY,
            name TEXT,
            symbol TEXT,
            creator_wallet TEXT,
            twitter_link TEXT,
            telegram_link TEXT,
            website_link TEXT,
            description TEXT,
            image_uri TEXT,
            is_graduated BOOLEAN DEFAULT 0,
            created_at TIMESTAMP,
            graduated_at TIMESTAMP,
            market_cap REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS developers (
            wallet TEXT PRIMARY KEY,
            twitter_handle TEXT,
            total_tokens INTEGER DEFAULT 0,
            graduated_tokens INTEGER DEFAULT 0,
            migration_percentage REAL DEFAULT 0,
            last_migration_at TIMESTAMP,
            last_token_launch_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT,
            dev_wallet TEXT,
            token_mint TEXT,
            data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed BOOLEAN DEFAULT 0
        )
    ''')
    
    conn.commit()
    conn.close()
    logger.info("Database initialized successfully")

# Pump.fun API Client
class PumpFunClient:
    """Client for Pump.fun API"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
    
    def fetch_coins(self, offset: int = 0, limit: int = 100, complete: bool = False) -> List[Dict]:
        """Fetch coins from Pump.fun API"""
        params = {
            "offset": offset,
            "limit": limit,
            "includeNsfw": "true"
        }
        if complete:
            params["complete"] = "true"
        
        try:
            response = self.session.get(f"{self.base_url}/coins", params=params, timeout=30)
            response.raise_for_status()
            return response.json() or []
        except Exception as e:
            logger.error(f"Error fetching coins: {e}")
            return []
    
    def fetch_migrated_coins(self, limit: int = 100) -> List[Dict]:
        """Fetch only migrated (graduated) coins"""
        return self.fetch_coins(offset=0, limit=limit, complete=True)
    
    def fetch_user_coins(self, address: str, limit: int = 1000) -> List[Dict]:
        """Fetch all coins created by a specific user"""
        params = {
            "offset": 0,
            "limit": limit,
            "includeNsfw": "true"
        }
        
        try:
            response = self.session.get(
                f"{self.base_url}/coins/user-created-coins/{address}",
                params=params,
                timeout=30
            )
            if response.status_code == 404:
                return []
            response.raise_for_status()
            return response.json() or []
        except Exception as e:
            logger.error(f"Error fetching user coins for {address}: {e}")
            return []

# Twitter handle extractor
def extract_twitter_handle(url: str) -> Optional[str]:
    """Extract Twitter handle from URL"""
    if not url:
        return None
    
    # Match twitter.com/username/status/... or x.com/username/status/...
    patterns = [
        r'(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)/status',
        r'(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)/?$',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            handle = match.group(1)
            if handle.lower() not in ['i', 'intent', 'share', 'hashtag']:
                return handle
    return None

# Initialize clients
pumpfun_client = PumpFunClient(PUMPFUN_API_URL)

# Database operations
class DatabaseOps:
    @staticmethod
    def get_connection():
        return sqlite3.connect(DATABASE_PATH)
    
    @staticmethod
    def save_token(token_data: Dict):
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO tokens 
            (mint, name, symbol, creator_wallet, twitter_link, telegram_link, website_link, 
             description, image_uri, is_graduated, created_at, graduated_at, market_cap, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            token_data.get("mint"),
            token_data.get("name"),
            token_data.get("symbol"),
            token_data.get("creator"),
            token_data.get("twitter"),
            token_data.get("telegram"),
            token_data.get("website"),
            token_data.get("description"),
            token_data.get("image_uri"),
            token_data.get("complete", False),
            datetime.fromtimestamp(token_data.get("created_timestamp", 0) / 1000).isoformat() if token_data.get("created_timestamp") else None,
            datetime.fromtimestamp(token_data.get("last_trade_timestamp", 0) / 1000).isoformat() if token_data.get("complete") and token_data.get("last_trade_timestamp") else None,
            token_data.get("market_cap")
        ))
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_token(mint: str) -> Optional[Dict]:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM tokens WHERE mint = ?', (mint,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return {
                "mint": row[0],
                "name": row[1],
                "symbol": row[2],
                "creator_wallet": row[3],
                "twitter_link": row[4],
                "telegram_link": row[5],
                "website_link": row[6],
                "description": row[7],
                "image_uri": row[8],
                "is_graduated": bool(row[9]),
                "created_at": row[10],
                "graduated_at": row[11],
                "market_cap": row[12]
            }
        return None
    
    @staticmethod
    def update_developer_stats(wallet: str):
        """Update developer statistics"""
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        
        # Count total and graduated tokens
        cursor.execute('''
            SELECT COUNT(*), SUM(CASE WHEN is_graduated = 1 THEN 1 ELSE 0 END) 
            FROM tokens WHERE creator_wallet = ?
        ''', (wallet,))
        result = cursor.fetchone()
        total_tokens = result[0] or 0
        graduated_tokens = result[1] or 0
        migration_percentage = (graduated_tokens / total_tokens * 100) if total_tokens > 0 else 0
        
        # Get last migration time
        cursor.execute('''
            SELECT MAX(graduated_at) FROM tokens 
            WHERE creator_wallet = ? AND is_graduated = 1
        ''', (wallet,))
        last_migration = cursor.fetchone()[0]
        
        # Get last token launch time
        cursor.execute('''
            SELECT MAX(created_at) FROM tokens WHERE creator_wallet = ?
        ''', (wallet,))
        last_launch = cursor.fetchone()[0]
        
        # Get twitter handle from any token
        cursor.execute('''
            SELECT twitter_link FROM tokens 
            WHERE creator_wallet = ? AND twitter_link IS NOT NULL LIMIT 1
        ''', (wallet,))
        twitter_result = cursor.fetchone()
        twitter_handle = extract_twitter_handle(twitter_result[0]) if twitter_result else None
        
        cursor.execute('''
            INSERT OR REPLACE INTO developers 
            (wallet, twitter_handle, total_tokens, graduated_tokens, migration_percentage, 
             last_migration_at, last_token_launch_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (wallet, twitter_handle, total_tokens, graduated_tokens, migration_percentage, 
              last_migration, last_launch))
        
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_developer(wallet: str) -> Optional[Dict]:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM developers WHERE wallet = ?', (wallet,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return {
                "wallet": row[0],
                "twitter_handle": row[1],
                "total_tokens": row[2],
                "graduated_tokens": row[3],
                "migration_percentage": row[4],
                "last_migration_at": row[5],
                "last_token_launch_at": row[6]
            }
        return None
    
    @staticmethod
    def get_top_devs_by_percentage(limit: int = 50) -> List[Dict]:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM developers 
            WHERE total_tokens >= 2
            ORDER BY migration_percentage DESC, graduated_tokens DESC
            LIMIT ?
        ''', (limit,))
        rows = cursor.fetchall()
        conn.close()
        return [{
            "wallet": row[0],
            "twitter_handle": row[1],
            "total_tokens": row[2],
            "migrated_tokens": row[3],
            "migration_percentage": row[4],
            "last_migration_at": row[5],
            "last_token_launch_at": row[6]
        } for row in rows]
    
    @staticmethod
    def get_top_devs_by_count(limit: int = 50) -> List[Dict]:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM developers 
            ORDER BY graduated_tokens DESC, migration_percentage DESC
            LIMIT ?
        ''', (limit,))
        rows = cursor.fetchall()
        conn.close()
        return [{
            "wallet": row[0],
            "twitter_handle": row[1],
            "total_tokens": row[2],
            "migrated_tokens": row[3],
            "migration_percentage": row[4],
            "last_migration_at": row[5],
            "last_token_launch_at": row[6]
        } for row in rows]
    
    @staticmethod
    def add_event(event_type: str, dev_wallet: str, token_mint: str, data: Dict):
        """Add a new event to the database"""
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO events (event_type, dev_wallet, token_mint, data)
            VALUES (?, ?, ?, ?)
        ''', (event_type, dev_wallet, token_mint, json.dumps(data)))
        conn.commit()
        conn.close()

# Background task to scan tokens
async def scan_pump_tokens():
    """Background task to scan Pump.fun tokens"""
    logger.info("Starting Pump.fun token scanner...")
    
    # Track seen tokens to avoid duplicates
    seen_tokens = set()
    
    while True:
        try:
            # Fetch recent migrated coins
            migrated_coins = pumpfun_client.fetch_migrated_coins(limit=50)
            
            logger.info(f"Fetched {len(migrated_coins)} migrated coins")
            
            for coin in migrated_coins:
                mint = coin.get("mint")
                if not mint or mint in seen_tokens:
                    continue
                
                seen_tokens.add(mint)
                
                # Save token to database
                DatabaseOps.save_token(coin)
                
                # Update developer stats
                creator = coin.get("creator")
                if creator:
                    DatabaseOps.update_developer_stats(creator)
                    
                    # Check if this is a new token from a tracked dev
                    dev = DatabaseOps.get_developer(creator)
                    if dev and dev.get("graduated_tokens", 0) > 1:
                        logger.info(f"✅ Tracked dev {creator[:8]}... launched {coin.get('symbol')}")
            
            # Also scan recent new tokens for monitoring
            recent_coins = pumpfun_client.fetch_coins(limit=50)
            for coin in recent_coins:
                mint = coin.get("mint")
                creator = coin.get("creator")
                
                if not mint or not creator:
                    continue
                
                # Check if creator has migration history
                dev = DatabaseOps.get_developer(creator)
                if dev and dev.get("graduated_tokens", 0) > 0:
                    # Check if this is a new token
                    existing = DatabaseOps.get_token(mint)
                    if not existing:
                        logger.info(f"🚨 NEW TOKEN from tracked dev {creator[:8]}...: {coin.get('symbol')}")
                        DatabaseOps.save_token(coin)
                        DatabaseOps.add_event("new_token_launch", creator, mint, {
                            "symbol": coin.get("symbol"),
                            "name": coin.get("name"),
                            "dev_migration_rate": dev.get("migration_percentage")
                        })
            
            logger.info(f"Scan complete. Total tokens in DB: {len(seen_tokens)}")
            
        except Exception as e:
            logger.error(f"Error in token scanner: {e}")
        
        await asyncio.sleep(120)  # Scan every 2 minutes

# WebSocket manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

# FastAPI app
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_database()
    asyncio.create_task(scan_pump_tokens())
    logger.info("Background tasks started")
    yield
    logger.info("Shutting down...")

app = FastAPI(
    title="Padre Trenches Dev Intel API",
    description="Backend for tracking Solana token developers via Pump.fun",
    version="3.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data models
class TokenInfo(BaseModel):
    mint: str
    name: Optional[str] = None
    symbol: Optional[str] = None
    creator_wallet: Optional[str] = None
    twitter_handle: Optional[str] = None
    twitter_link: Optional[str] = None
    migration_percentage: Optional[float] = None
    migration_count: Optional[int] = None
    last_migration_at: Optional[str] = None
    market_cap: Optional[float] = None

class DevInfo(BaseModel):
    wallet: str
    twitter_handle: Optional[str] = None
    total_tokens: int = 0
    migrated_tokens: int = 0
    migration_percentage: float = 0.0
    last_migration_at: Optional[str] = None
    last_token_launch_at: Optional[str] = None

class TopDevsResponse(BaseModel):
    devs: List[DevInfo]
    updated_at: str

# API Endpoints
@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "Padre Trenches Dev Intel API",
        "version": "3.0.0",
        "api": "Pump.fun v3"
    }

@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "database": "connected",
        "version": "3.0.0"
    }

@app.get("/api/token/{mint}", response_model=TokenInfo)
async def get_token_info(mint: str):
    """Get information about a specific token"""
    token = DatabaseOps.get_token(mint)
    
    if not token:
        # Try to fetch from Pump.fun API
        coins = pumpfun_client.fetch_coins(limit=1)
        # This is a simplified approach - in production you'd want a direct endpoint
        raise HTTPException(status_code=404, detail="Token not found")
    
    dev = DatabaseOps.get_developer(token.get("creator_wallet")) if token.get("creator_wallet") else None
    
    return TokenInfo(
        mint=token.get("mint"),
        name=token.get("name"),
        symbol=token.get("symbol"),
        creator_wallet=token.get("creator_wallet"),
        twitter_handle=dev.get("twitter_handle") if dev else extract_twitter_handle(token.get("twitter_link")),
        twitter_link=token.get("twitter_link"),
        migration_percentage=dev.get("migration_percentage") if dev else None,
        migration_count=dev.get("graduated_tokens") if dev else None,
        last_migration_at=dev.get("last_migration_at") if dev else None,
        market_cap=token.get("market_cap")
    )

@app.get("/api/dev/{wallet}", response_model=DevInfo)
async def get_dev_info(wallet: str):
    """Get information about a specific developer"""
    dev = DatabaseOps.get_developer(wallet)
    if not dev:
        # Try to fetch from Pump.fun API
        coins = pumpfun_client.fetch_user_coins(wallet)
        if not coins:
            raise HTTPException(status_code=404, detail="Developer not found")
        
        # Save coins and update stats
        for coin in coins:
            DatabaseOps.save_token(coin)
        DatabaseOps.update_developer_stats(wallet)
        dev = DatabaseOps.get_developer(wallet)
    
    return DevInfo(**dev)

@app.get("/api/top-devs/by-percentage", response_model=TopDevsResponse)
async def get_top_devs_by_percentage(limit: int = 50):
    """Get top developers sorted by migration percentage"""
    devs = DatabaseOps.get_top_devs_by_percentage(limit)
    return TopDevsResponse(
        devs=[DevInfo(**dev) for dev in devs],
        updated_at=datetime.utcnow().isoformat()
    )

@app.get("/api/top-devs/by-count", response_model=TopDevsResponse)
async def get_top_devs_by_count(limit: int = 50):
    """Get top developers sorted by migration count"""
    devs = DatabaseOps.get_top_devs_by_count(limit)
    return TopDevsResponse(
        devs=[DevInfo(**dev) for dev in devs],
        updated_at=datetime.utcnow().isoformat()
    )

@app.get("/api/events")
async def get_events(limit: int = 50):
    """Get recent unprocessed events"""
    conn = DatabaseOps.get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM events 
        WHERE processed = 0 
        ORDER BY created_at DESC 
        LIMIT ?
    ''', (limit,))
    rows = cursor.fetchall()
    conn.close()
    
    return [{
        "id": row[0],
        "event_type": row[1],
        "dev_wallet": row[2],
        "token_mint": row[3],
        "data": json.loads(row[4]) if row[4] else {},
        "created_at": row[5]
    } for row in rows]

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
