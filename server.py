"""
Padre Trenches Dev Intel - Backend Server v7.0
Tracks Solana token developers using PumpPortal + Helius + DexScreener

Architecture:
1. Token creation events → PumpPortal WebSocket (subscribeNewToken) - saves creator!
2. Migration events → PumpPortal WebSocket (subscribeMigration)
3. Token metadata (image, twitter) → DexScreener API
4. Twitter handle → Twitter API (from community links)
5. Fallback for creator → Helius parseTransaction
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
PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"
PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "14649a76-7c70-443c-b6da-41cffe2543fd")
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_API_URL = f"https://api.helius.xyz/v0"
TWITTER_API_KEY = "new1_defb379335c44d58890c0e2c59ada78f"
DEXSCREENER_API_URL = "https://api.dexscreener.com"
DATABASE_URL = os.getenv("DATABASE_URL", None)
DATABASE_PATH = os.getenv("DATABASE_PATH", "dev_intel.db")
USE_POSTGRES = DATABASE_URL is not None and POSTGRES_AVAILABLE

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============== Database Setup ==============

def init_database():
    """Initialize database with required tables"""
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
    else:
        conn = sqlite3.connect(DATABASE_PATH)
    
    cursor = conn.cursor()
    
    if USE_POSTGRES:
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
                is_graduated BOOLEAN DEFAULT FALSE,
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
                id SERIAL PRIMARY KEY,
                event_type TEXT,
                dev_wallet TEXT,
                token_mint TEXT,
                data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tokens_creator ON tokens(creator_wallet)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tokens_graduated ON tokens(is_graduated)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)')
    else:
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
                is_graduated INTEGER DEFAULT 0,
                created_at TEXT,
                graduated_at TEXT,
                market_cap REAL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS developers (
                wallet TEXT PRIMARY KEY,
                twitter_handle TEXT,
                total_tokens INTEGER DEFAULT 0,
                graduated_tokens INTEGER DEFAULT 0,
                migration_percentage REAL DEFAULT 0,
                last_migration_at TEXT,
                last_token_launch_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT,
                dev_wallet TEXT,
                token_mint TEXT,
                data TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tokens_creator ON tokens(creator_wallet)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tokens_graduated ON tokens(is_graduated)')
    
    conn.commit()
    conn.close()
    logger.info(f"Database initialized ({'PostgreSQL' if USE_POSTGRES else 'SQLite'})")


# ============== API Clients ==============

class DexScreenerClient:
    """Client for DexScreener API - get token info, image, socials"""
    
    def __init__(self):
        self.base_url = "https://api.dexscreener.com"
        self.session = requests.Session()
        self.session.headers.update({
            "accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; PumpTracker/1.0)"
        })
    
    def get_token_info(self, mint: str) -> Optional[Dict]:
        """Get token info from DexScreener including image and socials"""
        try:
            url = f"{self.base_url}/latest/dex/tokens/{mint}"
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                pairs = data.get("pairs", [])
                
                if pairs:
                    pair = pairs[0]
                    base_token = pair.get("baseToken", {})
                    info = pair.get("info", {})
                    socials = info.get("socials", [])
                    
                    # Find twitter/community link
                    twitter_link = None
                    website_link = None
                    for social in socials:
                        if social.get("type") == "twitter":
                            twitter_link = social.get("url")
                        elif social.get("type") == "website":
                            website_link = social.get("url")
                    
                    result = {
                        "mint": mint,
                        "name": base_token.get("name"),
                        "symbol": base_token.get("symbol"),
                        "image_uri": info.get("imageUrl"),
                        "twitter": twitter_link,
                        "website": website_link,
                        "price_usd": pair.get("priceUsd"),
                        "market_cap": pair.get("marketCap") or pair.get("fdv"),
                        "dex_id": pair.get("dexId")
                    }
                    
                    logger.info(f"[DexScreener] Got info for {mint[:8]}...: {result.get('symbol')}")
                    return result
                    
            return None
        except Exception as e:
            logger.error(f"[DexScreener] Error getting token info: {e}")
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


class HeliusClient:
    """Client for Helius API - parse transactions"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = f"https://api.helius.xyz/v0"
        self.rpc_url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"
        self.session = requests.Session()
    
    def parse_transaction(self, signature: str) -> Optional[Dict]:
        """Parse a transaction to get details"""
        try:
            url = f"{self.base_url}/transactions/?api-key={self.api_key}"
            response = self.session.post(
                url,
                json={"transactions": [signature]},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    return data[0]
            return None
        except Exception as e:
            logger.error(f"[Helius] Error parsing transaction: {e}")
            return None
    
    def get_token_transactions(self, mint: str, limit: int = 100) -> List[Dict]:
        """Get transactions for a token"""
        try:
            url = f"{self.base_url}/addresses/{mint}/transactions?api-key={self.api_key}&limit={limit}"
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                return response.json()
            return []
        except Exception as e:
            logger.error(f"[Helius] Error getting transactions: {e}")
            return []


# Initialize clients
dexscreener_client = DexScreenerClient()
twitter_client = TwitterClient(TWITTER_API_KEY)
helius_client = HeliusClient(HELIUS_API_KEY)


# ============== Database Operations ==============

class DatabaseOps:
    @staticmethod
    def get_connection():
        if USE_POSTGRES:
            return psycopg2.connect(DATABASE_URL)
        return sqlite3.connect(DATABASE_PATH)
    
    @staticmethod
    def execute(cursor, query: str, params: tuple = None):
        if USE_POSTGRES:
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
                token_data.get("created_timestamp") or token_data.get("created_at"),
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
    def update_developer_stats(wallet: str, increment_total: bool = False):
        """Update developer statistics"""
        try:
            conn = DatabaseOps.get_connection()
            cursor = conn.cursor()
            
            # Count total tokens created by this wallet
            DatabaseOps.execute(cursor, '''
                SELECT COUNT(*) FROM tokens WHERE creator_wallet = ?
            ''', (wallet,))
            total_tokens = cursor.fetchone()[0]
            
            # Count graduated tokens
            DatabaseOps.execute(cursor, '''
                SELECT COUNT(*) FROM tokens 
                WHERE creator_wallet = ? AND is_graduated = 1
            ''', (wallet,))
            graduated_tokens = cursor.fetchone()[0]
            
            # Calculate migration percentage
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
            logger.error(f"Error updating developer stats: {e}")
    
    @staticmethod
    def get_top_developers(sort_by: str = "percentage", limit: int = 50) -> List[Dict]:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        
        if sort_by == "percentage":
            order = "migration_percentage DESC, graduated_tokens DESC"
        else:
            order = "graduated_tokens DESC, migration_percentage DESC"
        
        DatabaseOps.execute(cursor, f'''
            SELECT wallet, twitter_handle, total_tokens, graduated_tokens, 
                   migration_percentage, last_migration_at, last_token_launch_at
            FROM developers 
            WHERE graduated_tokens > 0
            ORDER BY {order}
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
    
    @staticmethod
    def get_recent_events(limit: int = 50) -> List[Dict]:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        DatabaseOps.execute(cursor, '''
            SELECT id, event_type, dev_wallet, token_mint, data, created_at
            FROM events ORDER BY created_at DESC LIMIT ?
        ''', (limit,))
        rows = cursor.fetchall()
        conn.close()
        return [{
            "id": row[0], "event_type": row[1], "dev_wallet": row[2],
            "token_mint": row[3], "data": json.loads(row[4]) if row[4] else {},
            "created_at": row[5]
        } for row in rows]
    
    @staticmethod
    def get_db_stats() -> Dict:
        conn = DatabaseOps.get_connection()
        cursor = conn.cursor()
        
        DatabaseOps.execute(cursor, 'SELECT COUNT(*) FROM tokens')
        total_tokens = cursor.fetchone()[0]
        
        DatabaseOps.execute(cursor, 'SELECT COUNT(*) FROM tokens WHERE is_graduated = 1')
        graduated_tokens = cursor.fetchone()[0]
        
        DatabaseOps.execute(cursor, 'SELECT COUNT(*) FROM developers')
        total_devs = cursor.fetchone()[0]
        
        DatabaseOps.execute(cursor, 'SELECT COUNT(*) FROM developers WHERE graduated_tokens > 0')
        active_devs = cursor.fetchone()[0]
        
        conn.close()
        return {
            "total_tokens": total_tokens,
            "graduated_tokens": graduated_tokens,
            "total_developers": total_devs,
            "active_developers": active_devs
        }


# ============== PumpPortal WebSocket ==============

async def pumpportal_websocket_listener():
    """Listen to PumpPortal WebSocket for token creation and migration events"""
    logger.info("Starting PumpPortal WebSocket listener...")
    
    reconnect_delay = 1
    max_reconnect_delay = 60
    
    while True:
        try:
            async with websockets.connect(PUMPPORTAL_WS_URL) as ws:
                logger.info("Connected to PumpPortal WebSocket")
                reconnect_delay = 1
                
                # Subscribe to new token events (includes creator!)
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                logger.info("Subscribed to subscribeNewToken")
                
                # Subscribe to migration events
                await ws.send(json.dumps({"method": "subscribeMigration"}))
                logger.info("Subscribed to subscribeMigration")
                
                async for message in ws:
                    try:
                        data = json.loads(message)
                        
                        # Skip confirmation messages
                        if "message" in data:
                            logger.info(f"PumpPortal: {data['message']}")
                            continue
                        
                        # Handle token creation
                        if data.get("txType") == "create":
                            await process_token_creation(data)
                        
                        # Handle migration
                        elif data.get("txType") == "migrate":
                            await process_migration(data)
                            
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        logger.error(f"Error processing PumpPortal message: {e}")
                        
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"PumpPortal WebSocket closed: {e}")
        except Exception as e:
            logger.error(f"PumpPortal WebSocket error: {e}")
        
        logger.info(f"Reconnecting to PumpPortal in {reconnect_delay}s...")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)


async def process_token_creation(data: Dict):
    """Process token creation event from PumpPortal"""
    try:
        mint = data.get("mint")
        creator = data.get("traderPublicKey")
        name = data.get("name")
        symbol = data.get("symbol")
        uri = data.get("uri")
        market_cap = data.get("marketCapSol")
        
        if not mint or not creator:
            return
        
        logger.info(f"🆕 New token: {symbol} ({mint[:8]}...) by {creator[:8]}...")
        
        # Save token with creator
        token_data = {
            "mint": mint,
            "name": name,
            "symbol": symbol,
            "creator": creator,
            "image_uri": uri,  # Will be enriched later
            "market_cap": market_cap,
            "created_at": datetime.utcnow().isoformat(),
            "is_graduated": False
        }
        DatabaseOps.save_token(token_data)
        
        # Update developer stats
        DatabaseOps.update_developer_stats(creator, increment_total=True)
        
        # Add event
        DatabaseOps.add_event("token_created", creator, mint, {
            "symbol": symbol,
            "name": name
        })
        
    except Exception as e:
        logger.error(f"Error processing token creation: {e}")


async def process_migration(data: Dict):
    """Process migration event from PumpPortal"""
    try:
        mint = data.get("mint")
        signature = data.get("signature")
        pool = data.get("pool")  # "pump-amm"
        
        if not mint:
            return
        
        logger.info(f"🚀 Migration detected: {mint[:8]}... to {pool}")
        
        # Check if we have this token in DB (with creator)
        existing_token = DatabaseOps.get_token(mint)
        creator = existing_token.get("creator_wallet") if existing_token else None
        
        # Get token info from DexScreener
        dex_info = dexscreener_client.get_token_info(mint)
        
        # Prepare token data
        token_data = {
            "mint": mint,
            "name": dex_info.get("name") if dex_info else (existing_token.get("name") if existing_token else f"Token {mint[:8]}..."),
            "symbol": dex_info.get("symbol") if dex_info else (existing_token.get("symbol") if existing_token else "???"),
            "creator": creator,
            "image_uri": dex_info.get("image_uri") if dex_info else (existing_token.get("image_uri") if existing_token else None),
            "twitter": dex_info.get("twitter") if dex_info else (existing_token.get("twitter_link") if existing_token else None),
            "website": dex_info.get("website") if dex_info else (existing_token.get("website_link") if existing_token else None),
            "market_cap": dex_info.get("market_cap") if dex_info else (existing_token.get("market_cap") if existing_token else None),
            "is_graduated": True,
            "complete": True,
            "graduated_at": datetime.utcnow().isoformat()
        }
        
        # If no creator, try to find from Helius
        if not creator:
            logger.info(f"No creator in DB for {mint[:8]}..., trying Helius...")
            tx_data = helius_client.parse_transaction(signature)
            if tx_data:
                # feePayer might be the creator or a bot
                creator = tx_data.get("feePayer")
                token_data["creator"] = creator
        
        # Save token
        DatabaseOps.save_token(token_data)
        
        # Update developer stats if we have creator
        if creator:
            DatabaseOps.update_developer_stats(creator)
            
            # Add event
            DatabaseOps.add_event("migration", creator, mint, {
                "symbol": token_data.get("symbol"),
                "name": token_data.get("name"),
                "signature": signature,
                "pool": pool
            })
            
            logger.info(f"✅ Saved migration: {token_data.get('symbol')} by {creator[:8]}...")
        else:
            logger.warning(f"Could not find creator for {mint}")
            
    except Exception as e:
        logger.error(f"Error processing migration: {e}")


# ============== Background Tasks ==============

async def enrich_token_data():
    """Background task to enrich token data with DexScreener info"""
    while True:
        try:
            await asyncio.sleep(60)  # Run every minute
            
            conn = DatabaseOps.get_connection()
            cursor = conn.cursor()
            
            # Find tokens without image that are graduated
            DatabaseOps.execute(cursor, '''
                SELECT mint FROM tokens 
                WHERE is_graduated = 1 AND (image_uri IS NULL OR image_uri = '')
                LIMIT 20
            ''')
            tokens_to_enrich = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            if not tokens_to_enrich:
                continue
            
            logger.info(f"Enriching {len(tokens_to_enrich)} tokens...")
            
            for mint in tokens_to_enrich:
                try:
                    dex_info = dexscreener_client.get_token_info(mint)
                    if dex_info:
                        existing = DatabaseOps.get_token(mint)
                        if existing:
                            token_data = {
                                "mint": mint,
                                "name": dex_info.get("name") or existing.get("name"),
                                "symbol": dex_info.get("symbol") or existing.get("symbol"),
                                "creator": existing.get("creator_wallet"),
                                "image_uri": dex_info.get("image_uri"),
                                "twitter": dex_info.get("twitter") or existing.get("twitter_link"),
                                "website": dex_info.get("website") or existing.get("website_link"),
                                "market_cap": dex_info.get("market_cap") or existing.get("market_cap"),
                                "is_graduated": existing.get("is_graduated"),
                                "graduated_at": existing.get("graduated_at"),
                                "created_at": existing.get("created_at")
                            }
                            DatabaseOps.save_token(token_data)
                            
                            # Try to get twitter handle from community
                            twitter_link = dex_info.get("twitter")
                            if twitter_link and "/i/communities/" in twitter_link:
                                community_id = twitter_client.extract_community_id(twitter_link)
                                if community_id and existing.get("creator_wallet"):
                                    twitter_handle = twitter_client.get_community_admin(community_id)
                                    if twitter_handle:
                                        logger.info(f"Found twitter handle: @{twitter_handle}")
                    
                    await asyncio.sleep(1)  # Rate limit
                except Exception as e:
                    logger.error(f"Error enriching {mint[:8]}...: {e}")
                    
        except Exception as e:
            logger.error(f"Error in enrich task: {e}")


# ============== FastAPI App ==============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    init_database()
    
    # Start background tasks
    asyncio.create_task(pumpportal_websocket_listener())
    asyncio.create_task(enrich_token_data())
    
    logger.info("🚀 Server started with PumpPortal + DexScreener + Helius")
    yield
    logger.info("Server shutting down...")


app = FastAPI(
    title="Padre Trenches Dev Intel API",
    version="7.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============== API Endpoints ==============

@app.get("/")
async def root():
    return {
        "name": "Padre Trenches Dev Intel API",
        "version": "7.0.0",
        "status": "running",
        "api": "PumpPortal WebSocket + DexScreener + Helius",
        "endpoints": [
            "/api/top-devs/detailed",
            "/api/dev/{wallet}",
            "/api/dev/{wallet}/tokens",
            "/api/events",
            "/api/health",
            "/api/stats"
        ]
    }


@app.get("/api/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get("/api/stats")
async def get_stats():
    return DatabaseOps.get_db_stats()


@app.get("/api/top-devs/detailed")
async def get_top_devs_detailed(by: str = "percentage", limit: int = 50):
    """Get top developers with detailed stats"""
    developers = DatabaseOps.get_top_developers(sort_by=by, limit=limit)
    
    # Enrich with token images
    for dev in developers:
        tokens = DatabaseOps.get_dev_tokens(dev["wallet"], limit=5)
        dev["recent_tokens"] = tokens
        # Get first token with image
        for token in tokens:
            if token.get("image_uri"):
                dev["avatar"] = token["image_uri"]
                break
    
    return {"developers": developers, "count": len(developers)}


@app.get("/api/dev/{wallet}")
async def get_developer(wallet: str):
    """Get developer details"""
    conn = DatabaseOps.get_connection()
    cursor = conn.cursor()
    DatabaseOps.execute(cursor, 'SELECT * FROM developers WHERE wallet = ?', (wallet,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        raise HTTPException(status_code=404, detail="Developer not found")
    
    return {
        "wallet": row[0], "twitter_handle": row[1],
        "total_tokens": row[2], "migrated_tokens": row[3],
        "migration_percentage": row[4], "last_migration_at": row[5],
        "last_token_launch_at": row[6]
    }


@app.get("/api/dev/{wallet}/tokens")
async def get_developer_tokens(wallet: str, limit: int = 100):
    """Get tokens created by a developer"""
    tokens = DatabaseOps.get_dev_tokens(wallet, limit)
    return {"tokens": tokens, "count": len(tokens)}


@app.get("/api/events")
async def get_events(limit: int = 50):
    """Get recent events"""
    events = DatabaseOps.get_recent_events(limit)
    return {"events": events, "count": len(events)}


@app.post("/api/debug/force-enrich")
async def force_enrich(limit: int = 50):
    """Force enrich tokens with DexScreener data"""
    conn = DatabaseOps.get_connection()
    cursor = conn.cursor()
    
    DatabaseOps.execute(cursor, '''
        SELECT mint FROM tokens 
        WHERE is_graduated = 1
        ORDER BY graduated_at DESC
        LIMIT ?
    ''', (limit,))
    tokens = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    enriched = 0
    for mint in tokens:
        try:
            dex_info = dexscreener_client.get_token_info(mint)
            if dex_info:
                existing = DatabaseOps.get_token(mint)
                if existing:
                    token_data = {
                        "mint": mint,
                        "name": dex_info.get("name") or existing.get("name"),
                        "symbol": dex_info.get("symbol") or existing.get("symbol"),
                        "creator": existing.get("creator_wallet"),
                        "image_uri": dex_info.get("image_uri"),
                        "twitter": dex_info.get("twitter") or existing.get("twitter_link"),
                        "website": dex_info.get("website") or existing.get("website_link"),
                        "market_cap": dex_info.get("market_cap") or existing.get("market_cap"),
                        "is_graduated": existing.get("is_graduated"),
                        "graduated_at": existing.get("graduated_at"),
                        "created_at": existing.get("created_at")
                    }
                    DatabaseOps.save_token(token_data)
                    enriched += 1
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Error enriching {mint[:8]}...: {e}")
    
    return {"enriched": enriched, "total": len(tokens)}


# WebSocket for real-time updates
connected_clients: List[WebSocket] = []

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)


async def broadcast_event(event: Dict):
    """Broadcast event to all connected clients"""
    for client in connected_clients:
        try:
            await client.send_json(event)
        except:
            pass


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
