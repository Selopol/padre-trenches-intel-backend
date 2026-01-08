"""
Padre Trenches Dev Intel - Backend Server
Tracks Solana token developers using Helius WebSocket + Pump.fun API

Architecture:
1. Migrated tokens → Helius WebSocket (subscribe to PumpSwap program)
2. Token creator → pump.fun API: GET /coins/{tokenAddress}
3. Total tokens per wallet → pump.fun API: GET /balances/{walletAddress}
"""

import os
import json
import time
import asyncio
import logging
import sqlite3
import re
import base64
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from contextlib import asynccontextmanager

import requests
import websockets
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Configuration
PUMPFUN_API_URL = "https://frontend-api.pump.fun"
PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "14649a76-7c70-443c-b6da-41cffe2543fd")
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_WS_URL = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
TWITTER_API_KEY = "new1_defb379335c44d58890c0e2c59ada78f"
DATABASE_URL = os.getenv("DATABASE_URL", None)
DATABASE_PATH = os.getenv("DATABASE_PATH", "dev_intel.db")
USE_POSTGRES = DATABASE_URL is not None and POSTGRES_AVAILABLE

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database setup
def init_database():
    """Initialize database with required tables"""
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
    else:
        conn = sqlite3.connect(DATABASE_PATH)
    
    cursor = conn.cursor()
    
    if USE_POSTGRES:
        DatabaseOps.execute(cursor, '''
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
                is_graduated BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP,
                graduated_at TIMESTAMP,
                market_cap REAL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        DatabaseOps.execute(cursor, '''
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
        
        DatabaseOps.execute(cursor, '''
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                event_type TEXT,
                dev_wallet TEXT,
                token_mint TEXT,
                data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed BOOLEAN DEFAULT FALSE
            )
        ''')
    else:
        DatabaseOps.execute(cursor, '''
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
        
        DatabaseOps.execute(cursor, '''
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
        
        DatabaseOps.execute(cursor, '''
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
    
    # Create indexes
    if USE_POSTGRES:
        try:
            DatabaseOps.execute(cursor, 'CREATE INDEX IF NOT EXISTS idx_tokens_creator ON tokens(creator_wallet)')
            DatabaseOps.execute(cursor, 'CREATE INDEX IF NOT EXISTS idx_tokens_graduated ON tokens(is_graduated)')
            DatabaseOps.execute(cursor, 'CREATE INDEX IF NOT EXISTS idx_tokens_created ON tokens(created_at DESC)')
            DatabaseOps.execute(cursor, 'CREATE INDEX IF NOT EXISTS idx_devs_percentage ON developers(migration_percentage DESC)')
            DatabaseOps.execute(cursor, 'CREATE INDEX IF NOT EXISTS idx_devs_tokens ON developers(total_tokens)')
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")
    
    conn.commit()
    conn.close()
    db_type = "PostgreSQL" if USE_POSTGRES else "SQLite"
    logger.info(f"Database initialized successfully ({db_type})")


class PumpFunClient:
    """Client for Pump.fun API"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; PumpTracker/1.0)",
            "Accept": "application/json"
        })
    
    def get_token_info(self, mint: str) -> Optional[Dict]:
        """Get token info including creator from pump.fun API"""
        try:
            response = self.session.get(
                f"{self.base_url}/coins/{mint}",
                timeout=10
            )
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            logger.error(f"Error fetching token info for {mint}: {e}")
            return None
    
    def get_wallet_token_count(self, wallet: str) -> int:
        """Get total token count for a wallet using balances endpoint with pagination"""
        total_count = 0
        offset = 0
        limit = 200
        
        try:
            while True:
                response = self.session.get(
                    f"{self.base_url}/balances/{wallet}",
                    params={"limit": limit, "offset": offset},
                    timeout=30
                )
                
                if response.status_code != 200:
                    logger.warning(f"Balances API returned {response.status_code} for {wallet}")
                    break
                
                data = response.json()
                
                if isinstance(data, list):
                    batch_count = len(data)
                    total_count += batch_count
                    
                    if batch_count < limit:
                        break
                    offset += limit
                elif isinstance(data, dict):
                    # API might return {total: X, items: [...]}
                    if "total" in data:
                        return data["total"]
                    items = data.get("items", data.get("balances", []))
                    batch_count = len(items)
                    total_count += batch_count
                    
                    if batch_count < limit:
                        break
                    offset += limit
                else:
                    break
                
                # Safety limit
                if offset > 10000:
                    logger.warning(f"Hit safety limit for wallet {wallet}")
                    break
                    
        except Exception as e:
            logger.error(f"Error fetching token count for {wallet}: {e}")
        
        return total_count
    
    def fetch_user_created_coins(self, wallet: str, limit: int = 500) -> Dict:
        """Fetch coins created by user (fallback, has 500 limit)"""
        try:
            response = self.session.get(
                f"{self.base_url}/coins/user-created-coins/{wallet}",
                params={"limit": limit, "offset": 0, "includeNsfw": "true"},
                timeout=30
            )
            if response.status_code == 404:
                return {"coins": [], "count": 0}
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict):
                return data
            return {"coins": [], "count": 0}
        except Exception as e:
            logger.error(f"Error fetching user coins for {wallet}: {e}")
            return {"coins": [], "count": 0}


class HeliusClient:
    """Client for Helius RPC and WebSocket API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.rpc_url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"
        self.ws_url = f"wss://mainnet.helius-rpc.com/?api-key={api_key}"
        self.session = requests.Session()
    
    def get_transactions_for_address(self, address: str, before: str = None, limit: int = 100) -> List[Dict]:
        """Get transaction history for an address (without type filter to get all transactions)"""
        try:
            url = f"https://api.helius.xyz/v0/addresses/{address}/transactions"
            params = {"api-key": self.api_key, "limit": limit}
            if before:
                params["before"] = before
            
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code == 200:
                txs = response.json()
                logger.info(f"Helius API returned {len(txs)} transactions")
                return txs
            else:
                logger.error(f"Helius API error: {response.status_code}")
            return []
        except Exception as e:
            logger.error(f"Error fetching transactions: {e}")
            return []
    
    def find_pump_token_in_transaction(self, tx: Dict) -> Optional[str]:
        """Find pump token mint address in a transaction using multiple methods"""
        try:
            # Method 1: Through tokenTransfers (primary)
            if "tokenTransfers" in tx:
                for transfer in tx["tokenTransfers"]:
                    mint = transfer.get("mint", "")
                    if mint.endswith("pump"):
                        return mint
            
            # Method 2: Through accountData tokenBalanceChanges
            if "accountData" in tx:
                for account in tx["accountData"]:
                    # Check tokenBalanceChanges
                    for change in account.get("tokenBalanceChanges", []):
                        mint = change.get("mint", "")
                        if mint.endswith("pump"):
                            return mint
                    # Check tokenMint field
                    token_mint = account.get("tokenMint", "")
                    if token_mint.endswith("pump"):
                        return token_mint
            
            # Method 3: Through instructions accounts
            if "instructions" in tx:
                for ix in tx["instructions"]:
                    for account in ix.get("accounts", []):
                        if isinstance(account, str) and account.endswith("pump"):
                            return account
            
            return None
        except Exception as e:
            logger.error(f"Error finding pump token: {e}")
            return None
    
    def parse_migration_transaction(self, tx: Dict) -> Optional[Dict]:
        """Parse a transaction to extract migration info from Helius REST API format"""
        try:
            token_mint = self.find_pump_token_in_transaction(tx)
            
            if token_mint:
                return {
                    "token_mint": token_mint,
                    "signature": tx.get("signature"),
                    "slot": tx.get("slot"),
                    "timestamp": tx.get("timestamp"),
                    "type": tx.get("type", "UNKNOWN")
                }
            return None
        except Exception as e:
            logger.error(f"Error parsing transaction: {e}")
            return None


class TwitterClient:
    """Client for twitterapi.io"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.twitterapi.io"
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": api_key})
    
    def extract_community_id(self, url: str) -> Optional[str]:
        if not url:
            return None
        pattern = r'(?:twitter\.com|x\.com)/i/communities/(\d+)'
        match = re.search(pattern, url)
        return match.group(1) if match else None
    
    def get_community_admin(self, community_id: str) -> Optional[str]:
        try:
            url = f"{self.base_url}/twitter/community/info"
            response = self.session.get(url, params={"community_id": community_id}, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    admin = data.get("community_info", {}).get("admin", {})
                    return admin.get("screen_name") if admin else None
            return None
        except Exception as e:
            logger.error(f"Twitter API error: {e}")
            return None


def extract_twitter_handle(url: str) -> Optional[str]:
    if not url:
        return None
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
helius_client = HeliusClient(HELIUS_API_KEY)
twitter_client = TwitterClient(TWITTER_API_KEY)


class DatabaseOps:
    @staticmethod
    def get_connection():
        if USE_POSTGRES:
            return psycopg2.connect(DATABASE_URL)
        else:
            return sqlite3.connect(DATABASE_PATH)
    
    @staticmethod
    def execute(cursor, query, params=None):
        if USE_POSTGRES:
            # Only convert boolean comparisons for is_graduated and processed fields
            # Use regex-like specific replacements to avoid breaking numeric comparisons
            query = query.replace('is_graduated = 0', 'is_graduated = FALSE')
            query = query.replace('is_graduated = 1', 'is_graduated = TRUE')
            query = query.replace('processed = 0', 'processed = FALSE')
            query = query.replace('processed = 1', 'processed = TRUE')
            query = query.replace("'%", "'%%").replace("%'", "%%'")
            query = query.replace('?', '%s')
            if 'INSERT OR REPLACE INTO tokens' in query:
                query = query.replace('INSERT OR REPLACE INTO tokens', 
                    'INSERT INTO tokens').replace('VALUES (', 'VALUES (') + \
                    ' ON CONFLICT (mint) DO UPDATE SET ' + \
                    'name=EXCLUDED.name, symbol=EXCLUDED.symbol, creator_wallet=EXCLUDED.creator_wallet, ' + \
                    'twitter_link=EXCLUDED.twitter_link, telegram_link=EXCLUDED.telegram_link, ' + \
                    'website_link=EXCLUDED.website_link, description=EXCLUDED.description, ' + \
                    'image_uri=EXCLUDED.image_uri, is_graduated=EXCLUDED.is_graduated, ' + \
                    'created_at=EXCLUDED.created_at, graduated_at=EXCLUDED.graduated_at, ' + \
                    'market_cap=EXCLUDED.market_cap, updated_at=CURRENT_TIMESTAMP'
            elif 'INSERT OR REPLACE INTO developers' in query:
                query = query.replace('INSERT OR REPLACE INTO developers',
                    'INSERT INTO developers').replace('VALUES (', 'VALUES (') + \
                    ' ON CONFLICT (wallet) DO UPDATE SET ' + \
                    'twitter_handle=EXCLUDED.twitter_handle, total_tokens=EXCLUDED.total_tokens, ' + \
                    'graduated_tokens=EXCLUDED.graduated_tokens, migration_percentage=EXCLUDED.migration_percentage, ' + \
                    'last_migration_at=EXCLUDED.last_migration_at, last_token_launch_at=EXCLUDED.last_token_launch_at, ' + \
                    'updated_at=CURRENT_TIMESTAMP'
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor
    
    @staticmethod
    def save_token(token_data: Dict):
        try:
            conn = DatabaseOps.get_connection()
            cursor = conn.cursor()
            
            is_graduated = token_data.get("complete", False) or token_data.get("is_graduated", False)
            graduated_at = None
            if is_graduated:
                graduated_at = token_data.get("graduated_at") or datetime.utcnow().isoformat()
            
            DatabaseOps.execute(cursor, '''
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
                is_graduated,
                token_data.get("created_timestamp"),
                graduated_at,
                token_data.get("usd_market_cap") or token_data.get("market_cap")
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error saving token: {e}")
    
    @staticmethod
    def get_token(mint: str) -> Optional[Dict]:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        DatabaseOps.execute(cursor, 'SELECT * FROM tokens WHERE mint = ?', (mint,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return {
                "mint": row[0], "name": row[1], "symbol": row[2],
                "creator_wallet": row[3], "twitter_link": row[4],
                "telegram_link": row[5], "website_link": row[6],
                "description": row[7], "image_uri": row[8],
                "is_graduated": bool(row[9]), "created_at": row[10],
                "graduated_at": row[11], "market_cap": row[12]
            }
        return None
    
    @staticmethod
    def update_developer_stats(wallet: str):
        """Update developer statistics using pump.fun balances API for accurate count"""
        try:
            conn = DatabaseOps.get_connection()
            cursor = conn.cursor()
            
            # Get total token count from pump.fun balances API (no 500 limit)
            total_tokens = pumpfun_client.get_wallet_token_count(wallet)
            
            # If balances API fails, fall back to user-created-coins
            if total_tokens == 0:
                api_response = pumpfun_client.fetch_user_created_coins(wallet)
                total_tokens = api_response.get("count", 0)
                # Save tokens to database
                for token in api_response.get("coins", []):
                    if isinstance(token, dict):
                        DatabaseOps.save_token(token)
            
            # Count graduated tokens from database
            DatabaseOps.execute(cursor, '''
                SELECT COUNT(*) FROM tokens 
                WHERE creator_wallet = ? AND is_graduated = 1
            ''', (wallet,))
            result = cursor.fetchone()
            graduated_tokens = result[0] if result else 0
            
            migration_percentage = (graduated_tokens / total_tokens * 100) if total_tokens > 0 else 0
            
            # Get last migration time
            DatabaseOps.execute(cursor, '''
                SELECT MAX(graduated_at) FROM tokens 
                WHERE creator_wallet = ? AND is_graduated = 1
            ''', (wallet,))
            result = cursor.fetchone()
            last_migration = result[0] if result else None
            
            # Get last token launch time
            DatabaseOps.execute(cursor, '''
                SELECT MAX(created_at) FROM tokens WHERE creator_wallet = ?
            ''', (wallet,))
            result = cursor.fetchone()
            last_launch = result[0] if result else None
            
            # Get twitter handle from community links
            twitter_handle = None
            try:
                DatabaseOps.execute(cursor, '''
                    SELECT twitter_link FROM tokens 
                    WHERE creator_wallet = ? AND twitter_link LIKE ? LIMIT 1
                ''', (wallet, '%/i/communities/%'))
                twitter_result = cursor.fetchone()
                if twitter_result:
                    community_id = twitter_client.extract_community_id(twitter_result[0])
                    if community_id:
                        twitter_handle = twitter_client.get_community_admin(community_id)
            except Exception as e:
                logger.warning(f"Twitter API error for {wallet[:8]}...: {e}")
            
            DatabaseOps.execute(cursor, '''
                INSERT OR REPLACE INTO developers 
                (wallet, twitter_handle, total_tokens, graduated_tokens, migration_percentage, 
                 last_migration_at, last_token_launch_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (wallet, twitter_handle, total_tokens, graduated_tokens, migration_percentage, 
                  last_migration, last_launch))
            
            conn.commit()
            conn.close()
            logger.info(f"Updated dev {wallet[:8]}...: {graduated_tokens}/{total_tokens} = {migration_percentage:.1f}%")
        except Exception as e:
            logger.error(f"Error updating developer stats for {wallet[:8]}...: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    @staticmethod
    def get_developer(wallet: str) -> Optional[Dict]:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        DatabaseOps.execute(cursor, 'SELECT * FROM developers WHERE wallet = ?', (wallet,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return {
                "wallet": row[0], "twitter_handle": row[1],
                "total_tokens": row[2], "graduated_tokens": row[3],
                "migration_percentage": row[4], "last_migration_at": row[5],
                "last_token_launch_at": row[6]
            }
        return None
    
    @staticmethod
    def get_top_devs_by_percentage(limit: int = 50) -> List[Dict]:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        DatabaseOps.execute(cursor, '''
            SELECT * FROM developers 
            WHERE total_tokens >= 2
            ORDER BY migration_percentage DESC, graduated_tokens DESC
            LIMIT ?
        ''', (limit,))
        rows = cursor.fetchall()
        conn.close()
        return [{
            "wallet": row[0], "twitter_handle": row[1],
            "total_tokens": row[2], "migrated_tokens": row[3],
            "migration_percentage": row[4], "last_migration_at": row[5],
            "last_token_launch_at": row[6]
        } for row in rows]
    
    @staticmethod
    def get_top_devs_by_count(limit: int = 50) -> List[Dict]:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        DatabaseOps.execute(cursor, '''
            SELECT * FROM developers 
            WHERE graduated_tokens >= 1
            ORDER BY graduated_tokens DESC, migration_percentage DESC
            LIMIT ?
        ''', (limit,))
        rows = cursor.fetchall()
        conn.close()
        return [{
            "wallet": row[0], "twitter_handle": row[1],
            "total_tokens": row[2], "migrated_tokens": row[3],
            "migration_percentage": row[4], "last_migration_at": row[5],
            "last_token_launch_at": row[6]
        } for row in rows]
    
    @staticmethod
    def get_dev_tokens(wallet: str, limit: int = 100) -> List[Dict]:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        DatabaseOps.execute(cursor, '''
            SELECT * FROM tokens 
            WHERE creator_wallet = ? 
            ORDER BY created_at DESC 
            LIMIT ?
        ''', (wallet, limit))
        rows = cursor.fetchall()
        conn.close()
        return [{
            "mint": row[0], "name": row[1], "symbol": row[2],
            "creator_wallet": row[3], "twitter_link": row[4],
            "telegram_link": row[5], "website_link": row[6],
            "description": row[7], "image_uri": row[8],
            "is_graduated": bool(row[9]), "created_at": row[10],
            "graduated_at": row[11], "market_cap": row[12]
        } for row in rows]
    
    @staticmethod
    def add_event(event_type: str, dev_wallet: str, token_mint: str, data: Dict):
        try:
            conn = DatabaseOps.get_connection()
            cursor = conn.cursor()
            DatabaseOps.execute(cursor, '''
                INSERT INTO events (event_type, dev_wallet, token_mint, data)
                VALUES (?, ?, ?, ?)
            ''', (event_type, dev_wallet, token_mint, json.dumps(data)))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error adding event: {e}")


# Helius WebSocket for real-time migration tracking
async def helius_websocket_listener():
    """Listen to Helius WebSocket for PumpSwap migrations"""
    logger.info(f"Starting Helius WebSocket listener for PumpSwap: {PUMPSWAP_PROGRAM}")
    
    reconnect_delay = 1
    max_reconnect_delay = 60
    
    while True:
        try:
            async with websockets.connect(HELIUS_WS_URL) as ws:
                logger.info("Connected to Helius WebSocket")
                reconnect_delay = 1
                
                # Subscribe to PumpSwap program logs
                subscribe_msg = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [PUMPSWAP_PROGRAM]},
                        {"commitment": "confirmed"}
                    ]
                }
                await ws.send(json.dumps(subscribe_msg))
                logger.info(f"Subscribed to PumpSwap program: {PUMPSWAP_PROGRAM}")
                
                # Ping task to keep connection alive
                async def ping_task():
                    while True:
                        try:
                            await asyncio.sleep(30)
                            await ws.ping()
                        except:
                            break
                
                ping = asyncio.create_task(ping_task())
                
                try:
                    async for message in ws:
                        try:
                            data = json.loads(message)
                            
                            # Handle subscription confirmation
                            if "result" in data and isinstance(data["result"], int):
                                logger.info(f"Subscription confirmed: {data['result']}")
                                continue
                            
                            # Handle log notifications
                            if "method" in data and data["method"] == "logsNotification":
                                await process_migration_log(data)
                        except json.JSONDecodeError:
                            continue
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                finally:
                    ping.cancel()
                    
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        
        logger.info(f"Reconnecting in {reconnect_delay}s...")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)


async def process_migration_log(data: Dict):
    """Process a migration log from Helius WebSocket"""
    try:
        params = data.get("params", {})
        result = params.get("result", {})
        value = result.get("value", {})
        
        signature = value.get("signature")
        logs = value.get("logs", [])
        
        # Look for migration-related logs
        is_migration = False
        for log in logs:
            if "migrate" in log.lower() or "graduation" in log.lower():
                is_migration = True
                break
        
        if not is_migration:
            return
        
        logger.info(f"🚀 Migration detected! Signature: {signature}")
        
        # Fetch full transaction to get token details
        # Use Helius parsed transaction API
        try:
            response = requests.post(
                HELIUS_RPC_URL,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
                },
                timeout=10
            )
            
            if response.status_code == 200:
                tx_data = response.json().get("result", {})
                await extract_and_save_migration(tx_data, signature)
        except Exception as e:
            logger.error(f"Error fetching transaction {signature}: {e}")
            
    except Exception as e:
        logger.error(f"Error processing migration log: {e}")


async def extract_and_save_migration(tx_data: Dict, signature: str):
    """Extract token info from migration transaction and save"""
    try:
        if not tx_data:
            logger.warning(f"No transaction data for {signature}")
            return
            
        meta = tx_data.get("meta") or {}
        post_token_balances = meta.get("postTokenBalances") or []
        
        # Find the pump token
        token_mint = None
        for balance in post_token_balances:
            mint = balance.get("mint", "")
            if mint.endswith("pump"):
                token_mint = mint
                break
        
        if not token_mint:
            logger.warning(f"No pump token found in tx {signature}")
            return
        
        logger.info(f"Found migrated token: {token_mint}")
        
        # Get token creator from pump.fun API
        token_info = pumpfun_client.get_token_info(token_mint)
        
        if token_info:
            creator = token_info.get("creator")
            
            # Save token as graduated
            token_info["complete"] = True
            token_info["graduated_at"] = datetime.utcnow().isoformat()
            DatabaseOps.save_token(token_info)
            
            if creator:
                # Update developer stats
                DatabaseOps.update_developer_stats(creator)
                
                # Add event
                DatabaseOps.add_event("migration", creator, token_mint, {
                    "symbol": token_info.get("symbol"),
                    "name": token_info.get("name"),
                    "signature": signature
                })
                
                logger.info(f"✅ Saved migration: {token_info.get('symbol')} by {creator[:8]}...")
        else:
            logger.warning(f"Could not get token info for {token_mint}")
            
    except Exception as e:
        logger.error(f"Error extracting migration: {e}")


# Historical migration loader
async def load_historical_migrations():
    """Load historical migrations from Helius API"""
    logger.info("Loading historical migrations...")
    
    # Load existing tokens to avoid duplicates
    conn = DatabaseOps.get_connection()
    cursor = conn.cursor()
    DatabaseOps.execute(cursor, 'SELECT mint FROM tokens WHERE is_graduated = 1')
    existing_mints = set(row[0] for row in cursor.fetchall())
    conn.close()
    
    logger.info(f"Found {len(existing_mints)} existing migrated tokens")
    
    # If we already have tokens, skip historical load
    if len(existing_mints) > 100:
        logger.info("Skipping historical load - database already populated")
        return
    
    # Fetch recent transactions from PumpSwap program
    before = None
    total_loaded = 0
    max_pages = 50
    
    for page in range(max_pages):
        try:
            txs = helius_client.get_transactions_for_address(PUMPSWAP_PROGRAM, before=before, limit=100)
            
            if not txs:
                logger.info(f"No more transactions at page {page}")
                break
            
            # Debug: Log first transaction structure on first page
            if page == 0 and txs:
                first_tx = txs[0]
                logger.info(f"First tx type: {first_tx.get('type')}, has tokenTransfers: {'tokenTransfers' in first_tx}")
                logger.info(f"First tx keys: {list(first_tx.keys())[:10]}")
            
            pump_tokens_found = 0
            for tx in txs:
                signature = tx.get("signature")
                
                # Parse transaction for pump tokens
                migration = helius_client.parse_migration_transaction(tx)
                if migration:
                    pump_tokens_found += 1
                    token_mint = migration["token_mint"]
                    
                    if token_mint in existing_mints:
                        continue
                    
                    # Get token info
                    token_info = pumpfun_client.get_token_info(token_mint)
                    if token_info:
                        token_info["complete"] = True
                        token_info["graduated_at"] = datetime.fromtimestamp(
                            migration.get("timestamp", time.time())
                        ).isoformat()
                        DatabaseOps.save_token(token_info)
                        
                        creator = token_info.get("creator")
                        if creator:
                            DatabaseOps.update_developer_stats(creator)
                        
                        existing_mints.add(token_mint)
                        total_loaded += 1
                        
                        if total_loaded % 10 == 0:
                            logger.info(f"Loaded {total_loaded} historical migrations...")
            
            logger.info(f"Page {page}: Found {pump_tokens_found} pump tokens in {len(txs)} transactions")
            
            # Get cursor for next page
            if txs:
                before = txs[-1].get("signature")
            
            await asyncio.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Error loading historical migrations: {e}")
            break
    
    logger.info(f"Historical load complete: {total_loaded} migrations loaded")


# WebSocket manager for client connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
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
    
    # Start Helius WebSocket listener
    asyncio.create_task(helius_websocket_listener())
    
    # Load historical migrations (only on fresh database)
    asyncio.create_task(load_historical_migrations())
    
    logger.info("Background tasks started")
    yield
    logger.info("Shutting down...")

app = FastAPI(
    title="Padre Trenches Dev Intel API",
    description="Backend for tracking Solana token developers via Helius + Pump.fun",
    version="4.0.0",
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
        "version": "4.0.0",
        "api": "Helius WebSocket + Pump.fun"
    }

@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "database": "connected",
        "version": "4.0.0"
    }

@app.get("/api/debug/db-stats")
async def get_db_stats():
    try:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        
        DatabaseOps.execute(cursor, 'SELECT COUNT(*) FROM developers')
        devs_count = cursor.fetchone()[0]
        
        DatabaseOps.execute(cursor, 'SELECT COUNT(*) FROM tokens')
        tokens_count = cursor.fetchone()[0]
        
        DatabaseOps.execute(cursor, 'SELECT COUNT(*) FROM tokens WHERE is_graduated = 1')
        graduated_count = cursor.fetchone()[0]
        
        DatabaseOps.execute(cursor, 'SELECT COUNT(*) FROM events')
        events_count = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "status": "ok",
            "developers_count": devs_count,
            "tokens_count": tokens_count,
            "graduated_tokens": graduated_count,
            "events_count": events_count,
            "database_type": "PostgreSQL" if USE_POSTGRES else "SQLite"
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.get("/api/token/{mint}", response_model=TokenInfo)
async def get_token_info(mint: str):
    token = DatabaseOps.get_token(mint)
    
    if not token:
        # Try to fetch from Pump.fun API
        token_data = pumpfun_client.get_token_info(mint)
        if token_data:
            DatabaseOps.save_token(token_data)
            token = DatabaseOps.get_token(mint)
        else:
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
    dev = DatabaseOps.get_developer(wallet)
    if not dev:
        # Fetch and calculate stats
        DatabaseOps.update_developer_stats(wallet)
        dev = DatabaseOps.get_developer(wallet)
        if not dev:
            raise HTTPException(status_code=404, detail="Developer not found")
    
    return DevInfo(**dev)

@app.get("/api/dev/{wallet}/tokens")
async def get_dev_tokens(wallet: str, limit: int = 100):
    dev = DatabaseOps.get_developer(wallet)
    if not dev:
        DatabaseOps.update_developer_stats(wallet)
        dev = DatabaseOps.get_developer(wallet)
    
    tokens = DatabaseOps.get_dev_tokens(wallet, limit)
    
    return {
        "developer": dev,
        "tokens": tokens,
        "total_count": len(tokens)
    }

@app.get("/api/top-devs/by-percentage", response_model=TopDevsResponse)
async def get_top_devs_by_percentage(limit: int = 50):
    devs = DatabaseOps.get_top_devs_by_percentage(limit)
    return TopDevsResponse(
        devs=[DevInfo(**dev) for dev in devs],
        updated_at=datetime.utcnow().isoformat()
    )

@app.get("/api/top-devs/by-count", response_model=TopDevsResponse)
async def get_top_devs_by_count(limit: int = 50):
    devs = DatabaseOps.get_top_devs_by_count(limit)
    return TopDevsResponse(
        devs=[DevInfo(**dev) for dev in devs],
        updated_at=datetime.utcnow().isoformat()
    )

@app.get("/api/top-devs/detailed")
async def get_top_devs_detailed(limit: int = 50, by: str = "percentage"):
    if by == "count":
        devs = DatabaseOps.get_top_devs_by_count(limit)
    else:
        devs = DatabaseOps.get_top_devs_by_percentage(limit)
    
    return {
        "devs": devs,
        "updated_at": datetime.utcnow().isoformat()
    }

@app.get("/api/events")
async def get_events(limit: int = 50):
    conn = DatabaseOps.get_connection()
    cursor = conn.cursor()
    DatabaseOps.execute(cursor, '''
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
