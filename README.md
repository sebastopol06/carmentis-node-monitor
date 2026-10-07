# Carmentis Mainnet Node & Monitor

Public Carmentis mainnet node operated by **seba**, with a lightweight monitoring dashboard.

## Public endpoints

- Dashboard: https://carmentis-node-sba.fr/dashboard/
- CometBFT RPC status: https://carmentis-node-sba.fr/status
- Node moniker: `seba`
- Network: `cmts`

## Architecture

Docker Compose services:

- `node-cometbft` — CometBFT node
- `node-abci` — Carmentis ABCI application
- `carmentis-monitor` — monitoring API and SQLite statistics
- `caddy` — HTTPS reverse proxy and dashboard

Persistent blockchain data is stored locally in `abci/` and `cometbft/` and is **not versioned**.

## Useful commands

Start / update all services:

    docker compose up -d

Show containers:

    docker compose ps

Follow logs:

    docker compose logs -f

CometBFT logs:

    docker compose logs -f node-cometbft

ABCI logs:

    docker compose logs -f node-abci

Monitor logs:

    docker compose logs -f monitor

Rebuild the monitor after changing `monitor.py`:

    docker compose up -d --build monitor

Restart a service:

    docker compose restart node-cometbft

## Local checks

Node status:

    curl -s localhost:26657/status | jq

Latest block:

    curl -s localhost:26657/block | jq '.result.block.header.height'

Public endpoint:

    curl -s https://carmentis-node-sba.fr/status | jq '.result.sync_info'

Monitor API:

    curl -s https://carmentis-node-sba.fr/dashboard/api/stats | jq

Charts API:

    curl -s https://carmentis-node-sba.fr/dashboard/api/charts | jq

## Dashboard

Static frontend:

    dashboard/index.html

Monitor backend:

    monitor/monitor.py

The monitor keeps a rolling 7-day local observation window.

## Repository safety

The following are intentionally excluded from Git:

- ABCI database
- CometBFT blockchain data
- node/private keys
- monitor SQLite databases
- `.env` files and secrets

Never commit `abci/`, `cometbft/`, private keys or database files.
