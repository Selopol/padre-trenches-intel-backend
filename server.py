"""
Padre Trenches Dev Intel - Backend Server
24/7 service that tracks Solana token developers and their migrations
"""

import os
import json
import time
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, asdict
from contextlib import asynccontextmanager

import requests
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Configuration
HELIUS_RPC_URL = os.getenv("HELIUS_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=14649a76-7c70-443c-b6da-41cffe2543fd")
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY", "new1_defb379335c44d58890c0e2c59ada78f")
DATABASE_PATH = os.getenv("DATABASE_PATH", "dev_intel.db")

# Pump.fun Program IDs
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_AMM_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

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
    
    # Tokens table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            mint TEXT PRIMARY KEY,
            name TEXT,
            symbol TEXT,
            dev_wallet TEXT,
            twitter_handle TEXT,
            migrated_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Developers table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS developers (
            wallet TEXT PRIMARY KEY,
            twitter_handle TEXT,
            total_tokens INTEGER DEFAULT 0,
            migrated_tokens INTEGER DEFAULT 0,
            migration_percentage REAL DEFAULT 0,
            last_migration_at TIMESTAMP,
            last_token_launch_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Tracked wallets for real-time monitoring
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracked_wallets (
            wallet TEXT PRIMARY KEY,
            is_active BOOLEAN DEFAULT 1,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Events table for new token launches
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

# Helius RPC Client
class HeliusClient:
    """Client for interacting with Helius RPC"""
    
    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url
        self.session = requests.Session()
    
    def _rpc_call(self, method: str, params: List[Any] = None) -> Dict:
        """Make an RPC call to Helius"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params or []
        }
        try:
            response = self.session.post(self.rpc_url, json=payload, timeout=30)
            response.raise_for_status()
            result = response.json()
            if "error" in result:
                logger.error(f"RPC Error: {result['error']}")
                return None
            return result.get("result")
        except Exception as e:
            logger.error(f"RPC call failed: {e}")
            return None
    
    def get_signatures_for_address(self, address: str, limit: int = 1000, before: str = None) -> List[Dict]:
        """Get transaction signatures for an address"""
        params = [address, {"limit": limit}]
        if before:
            params[1]["before"] = before
        return self._rpc_call("getSignaturesForAddress", params) or []
    
    def get_transaction(self, signature: str) -> Optional[Dict]:
        """Get transaction details"""
        params = [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
        return self._rpc_call("getTransaction", params)
    
    def get_account_info(self, address: str) -> Optional[Dict]:
        """Get account info"""
        params = [address, {"encoding": "jsonParsed"}]
        return self._rpc_call("getAccountInfo", params)
    
    def get_token_accounts_by_owner(self, owner: str) -> List[Dict]:
        """Get token accounts owned by an address"""
        params = [
            owner,
            {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
            {"encoding": "jsonParsed"}
        ]
        result = self._rpc_call("getTokenAccountsByOwner", params)
        return result.get("value", []) if result else []
    
    def get_recent_pump_migrations(self, limit: int = 100) -> List[Dict]:
        """Get recent Pump AMM migrations"""
        # Get signatures from Pump AMM program
        signatures = self.get_signatures_for_address(PUMP_AMM_PROGRAM_ID, limit)
        migrations = []
        
        for sig_info in signatures[:limit]:
            tx = self.get_transaction(sig_info["signature"])
            if tx:
                migration_data = self._parse_migration_tx(tx, sig_info["signature"])
                if migration_data:
                    migrations.append(migration_data)
        
        return migrations
    
    def _parse_migration_tx(self, tx: Dict, signature: str) -> Optional[Dict]:
        """Parse a migration transaction to extract token and dev info"""
        try:
            meta = tx.get("meta", {})
            transaction = tx.get("transaction", {})
            message = transaction.get("message", {})
            
            # Get block time
            block_time = tx.get("blockTime")
            
            # Look for token mint in the transaction
            account_keys = message.get("accountKeys", [])
            
            # Try to find the token mint and dev wallet from instructions
            instructions = message.get("instructions", [])
            inner_instructions = meta.get("innerInstructions", [])
            
            token_mint = None
            dev_wallet = None
            
            # Parse account keys to find relevant addresses
            for i, key in enumerate(account_keys):
                if isinstance(key, dict):
                    pubkey = key.get("pubkey", "")
                else:
                    pubkey = key
                
                # First signer is typically the dev/creator
                if i == 0:
                    dev_wallet = pubkey
            
            # Look for token mint in post token balances
            post_token_balances = meta.get("postTokenBalances", [])
            for balance in post_token_balances:
                mint = balance.get("mint")
                if mint:
                    token_mint = mint
                    break
            
            if token_mint and dev_wallet:
                return {
                    "signature": signature,
                    "token_mint": token_mint,
                    "dev_wallet": dev_wallet,
                    "block_time": block_time,
                    "migrated_at": datetime.fromtimestamp(block_time).isoformat() if block_time else None
                }
            
            return None
        except Exception as e:
            logger.error(f"Error parsing migration tx: {e}")
            return None

# Twitter API Client (using API key format provided)
class TwitterClient:
    """Client for Twitter API interactions"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.twitter.com/2"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}"
        })
    
    def get_user_by_username(self, username: str) -> Optional[Dict]:
        """Get Twitter user by username"""
        try:
            url = f"{self.base_url}/users/by/username/{username}"
            params = {"user.fields": "description,profile_image_url,public_metrics,pinned_tweet_id"}
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.json().get("data")
            return None
        except Exception as e:
            logger.error(f"Twitter API error: {e}")
            return None
    
    def get_pinned_tweet(self, user_id: str) -> Optional[Dict]:
        """Get user's pinned tweet"""
        try:
            url = f"{self.base_url}/users/{user_id}"
            params = {
                "user.fields": "pinned_tweet_id",
                "expansions": "pinned_tweet_id",
                "tweet.fields": "author_id,text"
            }
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                includes = data.get("includes", {})
                tweets = includes.get("tweets", [])
                if tweets:
                    return tweets[0]
            return None
        except Exception as e:
            logger.error(f"Twitter API error getting pinned tweet: {e}")
            return None
    
    def search_token_twitter(self, token_name: str, token_symbol: str) -> Optional[str]:
        """Search for token's Twitter handle"""
        try:
            # Search for recent tweets mentioning the token
            query = f"{token_symbol} OR {token_name}"
            url = f"{self.base_url}/tweets/search/recent"
            params = {
                "query": query,
                "max_results": 10,
                "tweet.fields": "author_id",
                "expansions": "author_id",
                "user.fields": "username"
            }
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                includes = data.get("includes", {})
                users = includes.get("users", [])
                if users:
                    return users[0].get("username")
            return None
        except Exception as e:
            logger.error(f"Twitter search error: {e}")
            return None

# Data models
class TokenInfo(BaseModel):
    mint: str
    name: Optional[str] = None
    symbol: Optional[str] = None
    dev_wallet: Optional[str] = None
    twitter_handle: Optional[str] = None
    migration_percentage: Optional[float] = None
    migration_count: Optional[int] = None
    last_migration_at: Optional[str] = None

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

class NewTokenEvent(BaseModel):
    event_type: str = "new_token"
    dev_wallet: str
    twitter_handle: Optional[str] = None
    token_mint: str
    timestamp: str

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to WebSocket: {e}")

manager = ConnectionManager()

# Initialize clients
helius_client = HeliusClient(HELIUS_RPC_URL)
twitter_client = TwitterClient(TWITTER_API_KEY)

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
            INSERT OR REPLACE INTO tokens (mint, name, symbol, dev_wallet, twitter_handle, migrated_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            token_data.get("mint"),
            token_data.get("name"),
            token_data.get("symbol"),
            token_data.get("dev_wallet"),
            token_data.get("twitter_handle"),
            token_data.get("migrated_at")
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
                "dev_wallet": row[3],
                "twitter_handle": row[4],
                "migrated_at": row[5]
            }
        return None
    
    @staticmethod
    def save_developer(dev_data: Dict):
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO developers 
            (wallet, twitter_handle, total_tokens, migrated_tokens, migration_percentage, last_migration_at, last_token_launch_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            dev_data.get("wallet"),
            dev_data.get("twitter_handle"),
            dev_data.get("total_tokens", 0),
            dev_data.get("migrated_tokens", 0),
            dev_data.get("migration_percentage", 0),
            dev_data.get("last_migration_at"),
            dev_data.get("last_token_launch_at")
        ))
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
                "migrated_tokens": row[3],
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
            ORDER BY migration_percentage DESC 
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
            ORDER BY migrated_tokens DESC 
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
    def add_tracked_wallet(wallet: str):
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO tracked_wallets (wallet) VALUES (?)
        ''', (wallet,))
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_tracked_wallets() -> List[str]:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT wallet FROM tracked_wallets WHERE is_active = 1')
        rows = cursor.fetchall()
        conn.close()
        return [row[0] for row in rows]
    
    @staticmethod
    def save_event(event_data: Dict):
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO events (event_type, dev_wallet, token_mint, data)
            VALUES (?, ?, ?, ?)
        ''', (
            event_data.get("event_type"),
            event_data.get("dev_wallet"),
            event_data.get("token_mint"),
            json.dumps(event_data.get("data", {}))
        ))
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_unprocessed_events() -> List[Dict]:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, event_type, dev_wallet, token_mint, data, created_at 
            FROM events WHERE processed = 0 ORDER BY created_at DESC LIMIT 100
        ''')
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

# Background tasks
async def scan_migrations():
    """Background task to scan for new migrations"""
    logger.info("Starting migration scanner...")
    while True:
        try:
            migrations = helius_client.get_recent_pump_migrations(50)
            for migration in migrations:
                token_mint = migration.get("token_mint")
                dev_wallet = migration.get("dev_wallet")
                
                if not token_mint or not dev_wallet:
                    continue
                
                # Save token
                DatabaseOps.save_token({
                    "mint": token_mint,
                    "dev_wallet": dev_wallet,
                    "migrated_at": migration.get("migrated_at")
                })
                
                # Update developer stats
                dev = DatabaseOps.get_developer(dev_wallet)
                if dev:
                    dev["migrated_tokens"] = dev.get("migrated_tokens", 0) + 1
                    dev["total_tokens"] = max(dev.get("total_tokens", 0), dev["migrated_tokens"])
                    dev["migration_percentage"] = (dev["migrated_tokens"] / dev["total_tokens"]) * 100 if dev["total_tokens"] > 0 else 0
                    dev["last_migration_at"] = migration.get("migrated_at")
                else:
                    dev = {
                        "wallet": dev_wallet,
                        "total_tokens": 1,
                        "migrated_tokens": 1,
                        "migration_percentage": 100.0,
                        "last_migration_at": migration.get("migrated_at")
                    }
                
                DatabaseOps.save_developer(dev)
                
                # Add to tracked wallets if high migration rate
                if dev.get("migration_percentage", 0) >= 50 and dev.get("migrated_tokens", 0) >= 2:
                    DatabaseOps.add_tracked_wallet(dev_wallet)
            
            logger.info(f"Processed {len(migrations)} migrations")
        except Exception as e:
            logger.error(f"Error in migration scanner: {e}")
        
        await asyncio.sleep(60)  # Scan every minute

async def monitor_tracked_wallets():
    """Monitor tracked wallets for new token launches"""
    logger.info("Starting wallet monitor...")
    last_signatures = {}
    
    while True:
        try:
            tracked_wallets = DatabaseOps.get_tracked_wallets()
            
            for wallet in tracked_wallets[:20]:  # Limit to avoid rate limits
                signatures = helius_client.get_signatures_for_address(wallet, 5)
                
                if not signatures:
                    continue
                
                latest_sig = signatures[0]["signature"]
                
                # Check if this is a new transaction
                if wallet in last_signatures and last_signatures[wallet] != latest_sig:
                    # New transaction detected
                    tx = helius_client.get_transaction(latest_sig)
                    if tx:
                        # Check if it's a token creation
                        migration_data = helius_client._parse_migration_tx(tx, latest_sig)
                        if migration_data:
                            dev = DatabaseOps.get_developer(wallet)
                            event = {
                                "event_type": "new_token",
                                "dev_wallet": wallet,
                                "token_mint": migration_data.get("token_mint"),
                                "data": {
                                    "twitter_handle": dev.get("twitter_handle") if dev else None,
                                    "timestamp": datetime.utcnow().isoformat()
                                }
                            }
                            DatabaseOps.save_event(event)
                            
                            # Broadcast to connected clients
                            await manager.broadcast({
                                "type": "new_token",
                                "dev_wallet": wallet,
                                "twitter_handle": dev.get("twitter_handle") if dev else None,
                                "token_mint": migration_data.get("token_mint"),
                                "timestamp": datetime.utcnow().isoformat()
                            })
                            logger.info(f"New token from tracked dev: {wallet}")
                
                last_signatures[wallet] = latest_sig
                await asyncio.sleep(0.5)  # Rate limiting
        except Exception as e:
            logger.error(f"Error in wallet monitor: {e}")
        
        await asyncio.sleep(30)  # Check every 30 seconds

# FastAPI app
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    init_database()
    asyncio.create_task(scan_migrations())
    asyncio.create_task(monitor_tracked_wallets())
    logger.info("Background tasks started")
    yield
    # Shutdown
    logger.info("Shutting down...")

app = FastAPI(
    title="Padre Trenches Dev Intel API",
    description="Backend API for tracking Solana token developers and migrations",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Endpoints
@app.get("/")
async def root():
    return {"status": "ok", "service": "Padre Trenches Dev Intel API"}

@app.get("/api/token/{mint}", response_model=TokenInfo)
async def get_token_info(mint: str):
    """Get information about a specific token"""
    token = DatabaseOps.get_token(mint)
    if not token:
        # Try to fetch from chain
        raise HTTPException(status_code=404, detail="Token not found")
    
    dev = DatabaseOps.get_developer(token.get("dev_wallet")) if token.get("dev_wallet") else None
    
    return TokenInfo(
        mint=token.get("mint"),
        name=token.get("name"),
        symbol=token.get("symbol"),
        dev_wallet=token.get("dev_wallet"),
        twitter_handle=dev.get("twitter_handle") if dev else token.get("twitter_handle"),
        migration_percentage=dev.get("migration_percentage") if dev else None,
        migration_count=dev.get("migrated_tokens") if dev else None,
        last_migration_at=dev.get("last_migration_at") if dev else None
    )

@app.get("/api/dev/{wallet}", response_model=DevInfo)
async def get_dev_info(wallet: str):
    """Get information about a specific developer"""
    dev = DatabaseOps.get_developer(wallet)
    if not dev:
        raise HTTPException(status_code=404, detail="Developer not found")
    
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
async def get_events():
    """Get unprocessed events (for polling)"""
    events = DatabaseOps.get_unprocessed_events()
    return {
        "events": events,
        "updated_at": datetime.utcnow().isoformat()
    }

@app.post("/api/track-wallet/{wallet}")
async def track_wallet(wallet: str):
    """Add a wallet to the tracking list"""
    DatabaseOps.add_tracked_wallet(wallet)
    return {"status": "ok", "message": f"Wallet {wallet} added to tracking"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive and handle incoming messages
            data = await websocket.receive_text()
            # Echo back or handle commands
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "database": "connected"
    }

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
