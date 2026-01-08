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
MORALIS_API_KEY = os.getenv("MORALIS_API_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6IjhlMTFlNjEyLTNiMzUtNDAyMS04M2UxLWYwYWZiZWZmOWZkNyIsIm9yZ0lkIjoiNDg5MzA5IiwidXNlcklkIjoiNTAzNDQwIiwidHlwZUlkIjoiMmViNWQyNjEtMTg4MS00Mjc3LWJlYjAtMDBmYWVhNmUxZTUzIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3Njc4OTIzNzIsImV4cCI6NDkyMzY1MjM3Mn0.9AYBgfJ9NEQ-BVGCeD7oGlJftXm6T6LQPOBRZ28DI2A")
MORALIS_BASE_URL = "https://solana-gateway.moralis.io"
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
    
    def fetch_with_retry(self, url: str, params: Dict = None, max_retries: int = 5) -> Optional[Dict]:
        """Fetch URL with retry logic for rate limiting"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response.json()
                
                if response.status_code in [429, 503, 530, 1016]:  # Rate limit / Cloudflare
                    wait_time = 60 * (attempt + 1)  # 60s, 120s, 180s...
                    logger.warning(f"Rate limited ({response.status_code}), waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                
                logger.warning(f"API returned {response.status_code} for {url}")
                return None
                
            except Exception as e:
                logger.error(f"Request error (attempt {attempt+1}): {e}")
                time.sleep(10)
        
        return None
    
    def get_graduated_tokens(self, limit: int = 50, offset: int = 0) -> List[Dict]:
        """Get graduated (completed) tokens from pump.fun API"""
        try:
            # Use completed=true filter to get only graduated tokens
            params = {
                "limit": limit,
                "offset": offset,
                "sort": "last_trade_timestamp",
                "order": "DESC",
                "includeNsfw": "true",
                "completed": "true"  # Only graduated tokens
            }
            
            data = self.fetch_with_retry(f"{self.base_url}/coins", params=params)
            
            if data and isinstance(data, list):
                return data
            elif data and isinstance(data, dict):
                return data.get("coins", data.get("items", []))
            return []
            
        except Exception as e:
            logger.error(f"Error fetching graduated tokens: {e}")
            return []
    
    def get_king_of_hill_tokens(self) -> List[Dict]:
        """Get king-of-the-hill tokens (recently graduated)"""
        try:
            data = self.fetch_with_retry(
                f"{self.base_url}/coins/king-of-the-hill",
                params={"includeNsfw": "true"}
            )
            
            if data and isinstance(data, list):
                return data
            return []
            
        except Exception as e:
            logger.error(f"Error fetching king-of-hill tokens: {e}")
            return []


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


class MoralisClient:
    """Client for Moralis Solana API - used to get token creator via first swap"""
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "accept": "application/json",
            "X-API-Key": api_key
        })
    
    def get_token_creator(self, mint: str) -> Optional[str]:
        """Get token creator by finding the first swap (creator is the first buyer)"""
        try:
            url = f"{self.base_url}/token/mainnet/{mint}/swaps?limit=1&order=ASC"
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("result") and len(data["result"]) > 0:
                    first_swap = data["result"][0]
                    creator = first_swap.get("walletAddress")
                    logger.info(f"[Moralis] Found creator for {mint[:8]}...: {creator[:8] if creator else 'N/A'}...")
                    return creator
            else:
                logger.warning(f"[Moralis] Error fetching first swap: {response.status_code}")
            return None
        except Exception as e:
            logger.error(f"[Moralis] Error getting token creator: {e}")
            return None
    
    def get_graduated_tokens(self, limit: int = 50) -> List[Dict]:
        """Get graduated tokens from Moralis API"""
        try:
            url = f"{self.base_url}/token/mainnet/exchange/pumpfun/graduated?limit={limit}"
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                tokens = data.get("result", [])
                logger.info(f"[Moralis] Fetched {len(tokens)} graduated tokens")
                return tokens
            else:
                logger.warning(f"[Moralis] Error fetching graduated tokens: {response.status_code}")
            return []
        except Exception as e:
            logger.error(f"[Moralis] Error fetching graduated tokens: {e}")
            return []
    
    def get_token_metadata(self, mint: str) -> Optional[Dict]:
        """Get token metadata from Moralis"""
        try:
            url = f"{self.base_url}/token/mainnet/{mint}/metadata"
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            logger.error(f"[Moralis] Error getting token metadata: {e}")
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
moralis_client = MoralisClient(MORALIS_API_KEY, MORALIS_BASE_URL)


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
        
        # Get token creator - try pump.fun first, then Moralis as fallback
        token_info = pumpfun_client.get_token_info(token_mint)
        creator = None
        
        if token_info:
            creator = token_info.get("creator")
            # Save token as graduated
            token_info["complete"] = True
            token_info["graduated_at"] = datetime.utcnow().isoformat()
            DatabaseOps.save_token(token_info)
        else:
            logger.warning(f"Pump.fun API failed for {token_mint}, trying Moralis...")
            # Fallback to Moralis for creator
            creator = moralis_client.get_token_creator(token_mint)
            
            if creator:
                # Get metadata from Moralis
                moralis_meta = moralis_client.get_token_metadata(token_mint)
                token_info = {
                    "mint": token_mint,
                    "name": moralis_meta.get("name") if moralis_meta else f"Token {token_mint[:8]}...",
                    "symbol": moralis_meta.get("symbol") if moralis_meta else "???",
                    "creator": creator,
                    "complete": True,
                    "is_graduated": True,
                    "graduated_at": datetime.utcnow().isoformat()
                }
                DatabaseOps.save_token(token_info)
                logger.info(f"[Moralis] Got creator for {token_mint[:8]}...: {creator[:8]}...")
        
        if creator:
            # Update developer stats
            DatabaseOps.update_developer_stats(creator)
            
            # Add event
            DatabaseOps.add_event("migration", creator, token_mint, {
                "symbol": token_info.get("symbol") if token_info else "???",
                "name": token_info.get("name") if token_info else f"Token {token_mint[:8]}...",
                "signature": signature
            })
            
            logger.info(f"✅ Saved migration: {token_info.get('symbol') if token_info else token_mint[:8]} by {creator[:8]}...")
        else:
            logger.warning(f"Could not get creator for {token_mint} from any source")
            
    except Exception as e:
        logger.error(f"Error extracting migration: {e}")


# Historical migration loader - HELIUS ONLY (no pump.fun dependency)
async def load_historical_migrations():
    """Load historical migrations from Helius API only (pump.fun blocked)"""
    logger.info("=== Starting historical migrations load (Helius-only) ===")
    
    # Load existing tokens to avoid duplicates
    try:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        DatabaseOps.execute(cursor, 'SELECT mint FROM tokens WHERE is_graduated = 1')
        existing_mints = set(row[0] for row in cursor.fetchall())
        conn.close()
        logger.info(f"Found {len(existing_mints)} existing migrated tokens in DB")
    except Exception as e:
        logger.error(f"Error loading existing tokens: {e}")
        existing_mints = set()
    
    # If we already have tokens, skip historical load
    if len(existing_mints) > 100:
        logger.info("Skipping historical load - database already populated")
        return
    
    # Use Helius API to get PumpSwap transactions
    logger.info(f"Fetching PumpSwap transactions from Helius API...")
    logger.info(f"Program: {PUMPSWAP_PROGRAM}")
    
    total_loaded = 0
    before = None
    max_pages = 100  # Up to 10,000 transactions
    unique_tokens = set()
    
    for page in range(max_pages):
        try:
            logger.info(f"Requesting page {page} from Helius...")
            
            txs = helius_client.get_transactions_for_address(PUMPSWAP_PROGRAM, before=before, limit=100)
            
            if not txs:
                logger.info(f"No more transactions at page {page}")
                break
            
            logger.info(f"Got {len(txs)} transactions from Helius")
            
            # Debug first transaction on first page
            if page == 0 and txs:
                first_tx = txs[0]
                logger.info(f"First tx keys: {list(first_tx.keys())}")
                logger.info(f"First tx type: {first_tx.get('type')}")
            
            page_tokens = 0
            for tx in txs:
                # Find pump token in transaction
                token_mint = helius_client.find_pump_token_in_transaction(tx)
                
                if token_mint and token_mint not in existing_mints and token_mint not in unique_tokens:
                    unique_tokens.add(token_mint)
                    
                    # Save token with minimal data (Helius-only, no pump.fun)
                    timestamp = tx.get("timestamp", int(time.time()))
                    token_data = {
                        "mint": token_mint,
                        "name": f"Token {token_mint[:8]}...",  # Placeholder
                        "symbol": "???",  # Will be enriched later
                        "creator": None,  # Will be enriched later
                        "complete": True,
                        "is_graduated": True,
                        "graduated_at": datetime.fromtimestamp(timestamp).isoformat()
                    }
                    
                    DatabaseOps.save_token(token_data)
                    total_loaded += 1
                    page_tokens += 1
                    
                    if total_loaded % 50 == 0:
                        logger.info(f"Loaded {total_loaded} migrated tokens...")
            
            logger.info(f"Page {page}: Found {page_tokens} new pump tokens")
            
            # Get cursor for next page
            if txs:
                before = txs[-1].get("signature")
            
            # Rate limiting
            await asyncio.sleep(0.3)
            
        except Exception as e:
            logger.error(f"Error loading historical migrations: {e}")
            import traceback
            logger.error(traceback.format_exc())
            await asyncio.sleep(5)
            continue
    
    logger.info(f"Historical load complete: {total_loaded} migrated tokens loaded")
    logger.info(f"Note: Token metadata will be enriched when pump.fun API becomes available")


# Background task to enrich token data from pump.fun
async def enrich_token_data():
    """Try to enrich tokens that have no creator data"""
    while True:
        try:
            await asyncio.sleep(60)  # Run every 1 minute
            
            conn = DatabaseOps.get_connection()
            cursor = conn.cursor()
            DatabaseOps.execute(cursor, 
                'SELECT mint FROM tokens WHERE creator_wallet IS NULL AND is_graduated = 1 LIMIT 50'
            )
            tokens_to_enrich = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            if not tokens_to_enrich:
                continue
            
            logger.info(f"Attempting to enrich {len(tokens_to_enrich)} tokens...")
            
            for mint in tokens_to_enrich:
                creator = None
                token_info = pumpfun_client.get_token_info(mint)
                
                if token_info and token_info.get("creator"):
                    # Got data from pump.fun
                    creator = token_info.get("creator")
                    token_info["complete"] = True
                    token_info["is_graduated"] = True
                    DatabaseOps.save_token(token_info)
                else:
                    # Fallback to Moralis
                    creator = moralis_client.get_token_creator(mint)
                    if creator:
                        moralis_meta = moralis_client.get_token_metadata(mint)
                        token_info = {
                            "mint": mint,
                            "name": moralis_meta.get("name") if moralis_meta else f"Token {mint[:8]}...",
                            "symbol": moralis_meta.get("symbol") if moralis_meta else "???",
                            "creator": creator,
                            "complete": True,
                            "is_graduated": True
                        }
                        DatabaseOps.save_token(token_info)
                        logger.info(f"[Moralis] Enriched token {mint[:8]}... with creator {creator[:8]}...")
                
                if creator:
                    DatabaseOps.update_developer_stats(creator)
                    logger.info(f"Enriched token {mint[:8]}... with creator {creator[:8]}...")
                
                await asyncio.sleep(2)  # Rate limit
                
        except Exception as e:
            logger.error(f"Error enriching tokens: {e}")
            await asyncio.sleep(60)


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
    logger.info("Background tasks started")
    
    # Start Helius WebSocket listener
    logger.info(f"Starting Helius WebSocket listener for PumpSwap: {PUMPSWAP_PROGRAM}")
    asyncio.create_task(helius_websocket_listener())
    
    # Load historical migrations (only on fresh database)
    logger.info("Scheduling historical migrations load...")
    asyncio.create_task(load_historical_migrations())
    
    # Start background enrichment task (tries to get data from pump.fun when available)
    logger.info("Starting token enrichment background task...")
    asyncio.create_task(enrich_token_data())
    
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
        "version": "5.0.0",
        "api": "Helius WebSocket + Pump.fun + Moralis"
    }

@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "database": "connected",
        "version": "5.0.0"
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

@app.post("/api/debug/force-enrich")
async def force_enrich_tokens(limit: int = 100):
    """Force enrich tokens without creator using Moralis API"""
    try:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        DatabaseOps.execute(cursor, 
            f'SELECT mint FROM tokens WHERE creator_wallet IS NULL AND is_graduated = 1 LIMIT {limit}'
        )
        tokens_to_enrich = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        if not tokens_to_enrich:
            return {"status": "ok", "message": "No tokens to enrich", "enriched": 0}
        
        enriched = 0
        failed = 0
        
        for mint in tokens_to_enrich:
            try:
                # Use Moralis directly (faster than pump.fun)
                creator = moralis_client.get_token_creator(mint)
                
                if creator:
                    moralis_meta = moralis_client.get_token_metadata(mint)
                    token_info = {
                        "mint": mint,
                        "name": moralis_meta.get("name") if moralis_meta else f"Token {mint[:8]}...",
                        "symbol": moralis_meta.get("symbol") if moralis_meta else "???",
                        "creator": creator,
                        "complete": True,
                        "is_graduated": True
                    }
                    DatabaseOps.save_token(token_info)
                    DatabaseOps.update_developer_stats(creator)
                    enriched += 1
                    logger.info(f"[Force-Enrich] {mint[:8]}... -> {creator[:8]}...")
                else:
                    failed += 1
                
                await asyncio.sleep(0.5)  # Rate limit
            except Exception as e:
                logger.error(f"Error enriching {mint}: {e}")
                failed += 1
        
        return {
            "status": "ok",
            "total": len(tokens_to_enrich),
            "enriched": enriched,
            "failed": failed
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
