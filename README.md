# Padre Trenches Dev Intel - Backend

Backend API for tracking Solana token developers and their migration statistics.

## Deploy to Railway

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template)

### Quick Deploy

1. Fork this repository
2. Connect to Railway
3. Set environment variables (optional - defaults are embedded):
   - `HELIUS_RPC_URL` - Helius RPC endpoint
   - `TWITTER_API_KEY` - Twitter API bearer token
4. Deploy!

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PORT` | No | Server port (auto-set by Railway) |
| `HELIUS_RPC_URL` | No | Helius RPC URL (default embedded) |
| `TWITTER_API_KEY` | No | Twitter API key (default embedded) |
| `DATABASE_PATH` | No | SQLite DB path (default: dev_intel.db) |

## API Endpoints

- `GET /` - Service status
- `GET /api/health` - Health check
- `GET /api/token/{mint}` - Token info
- `GET /api/dev/{wallet}` - Developer info
- `GET /api/top-devs/by-percentage?limit=50` - Top devs by migration %
- `GET /api/top-devs/by-count?limit=50` - Top devs by count
- `GET /api/events` - Unprocessed events
- `WS /ws` - WebSocket for real-time updates

## Local Development

```bash
pip install -r requirements.txt
python server.py
```

Server runs on http://localhost:8080
