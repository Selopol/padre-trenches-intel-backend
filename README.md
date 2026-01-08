# Padre Trenches Dev Intel - Backend

Backend API for tracking Solana token developers using Pump.fun API v3.

## Features

- ✅ Tracks migrated Pump.fun tokens
- ✅ Identifies token creators and their migration statistics
- ✅ Extracts Twitter/Telegram/Website links from token metadata
- ✅ Real-time monitoring of new token launches from tracked developers
- ✅ WebSocket support for live updates
- ✅ REST API for Chrome extension integration

## Deploy to Railway

1. Fork or import this repository to your GitHub
2. Connect to [Railway](https://railway.app)
3. Create new project from GitHub repo
4. Railway will auto-detect Python and deploy
5. Copy your Railway app URL (e.g., `https://your-app.up.railway.app`)

## Environment Variables

All API keys are embedded in the code for easy deployment. No environment variables required!

| Variable | Required | Description |
|----------|----------|-------------|
| `PORT` | No | Server port (auto-set by Railway) |

## API Endpoints

### Health & Status
- `GET /` - Service status
- `GET /api/health` - Health check

### Token Information
- `GET /api/token/{mint}` - Get token info with dev statistics
  - Returns: name, symbol, creator, Twitter links, migration stats

### Developer Information  
- `GET /api/dev/{wallet}` - Get developer statistics
  - Returns: total tokens, migrated tokens, migration %, Twitter handle

### Top Developers
- `GET /api/top-devs/by-percentage?limit=50` - Top devs by migration %
- `GET /api/top-devs/by-count?limit=50` - Top devs by migration count

### Real-time Updates
- `GET /api/events?limit=50` - Recent new token launch events
- `WS /ws` - WebSocket for real-time notifications

## Example API Calls

```bash
# Get token info
curl https://your-app.up.railway.app/api/token/BtNNKoW24ETn3U7v7xmSHJMwv6qbqfvfdwCLUkifpump

# Get developer info
curl https://your-app.up.railway.app/api/dev/3fLG1UZzjnpN8XaZU7YwFB9QKZytp6vMKh1ArehKuumb

# Get top developers
curl https://your-app.up.railway.app/api/top-devs/by-percentage?limit=10
```

## How It Works

1. **Scans Pump.fun API** every 2 minutes for migrated tokens
2. **Extracts metadata** including creator wallet and social links
3. **Calculates statistics** for each developer (migration rate, total tokens)
4. **Monitors new launches** from developers with migration history
5. **Sends notifications** via WebSocket when tracked devs launch new tokens

## Data Sources

- **Pump.fun API v3**: `https://frontend-api-v3.pump.fun`
  - `/coins?complete=true` - Migrated tokens
  - `/coins/user-created-coins/{address}` - User's tokens

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run server
python server.py

# Server runs on http://localhost:8080
```

## Tech Stack

- **FastAPI** - Modern Python web framework
- **SQLite** - Embedded database
- **Requests** - HTTP client for Pump.fun API
- **Uvicorn** - ASGI server

## Database Schema

### tokens
- mint (PK), name, symbol, creator_wallet
- twitter_link, telegram_link, website_link
- is_graduated, created_at, graduated_at
- market_cap, image_uri

### developers  
- wallet (PK), twitter_handle
- total_tokens, graduated_tokens, migration_percentage
- last_migration_at, last_token_launch_at

### events
- id (PK), event_type, dev_wallet, token_mint
- data (JSON), created_at, processed

## License

MIT
