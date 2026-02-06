# Remote Deployment Guide

## Overview
Hoard can run as a remote, single-tenant service for local or external agents.

Key points:
- Use HTTPS via reverse proxy (recommended).
- Hoard itself serves HTTP behind the proxy.
- Remote bind requires explicit opt-in (`--allow-remote` or `server.allow_remote: true`).
- Health endpoint: `GET /health`.
- v1 outage behavior: best-effort retries only (no durable offline queue).

## Data Directory
All default runtime paths are derived from `HOARD_DATA_DIR`:
- `config.yaml`
- `hoard.db`
- `server.key`
- `artifacts/`
- daemon pid/log and sync lock files

Default if unset: `~/.hoard`.

## Recommended Remote Setup (Reverse Proxy + TLS)
1. Copy deploy assets:
- `deploy/docker-compose.yml`
- `deploy/Caddyfile`
- `deploy/.env.example`
2. Set environment:
```bash
cp deploy/.env.example deploy/.env
# edit HOARD_DOMAIN and HOARD_SERVER_SECRET
```
3. Start stack:
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env up -d --build
```
4. Verify:
```bash
curl -s https://$HOARD_DOMAIN/health
```

## SSH Tunnel Alternative
If you do not want to expose Hoard publicly:
```bash
ssh -N -L 19850:127.0.0.1:19850 user@remote-host
```
Then point clients at `http://127.0.0.1:19850/mcp`.

## Client Onboarding
Recommended flow (manual token provisioning):
1. On Hoard host:
```bash
hoard tokens add laptop-codex --scopes search,get,memory,sync,ingest
```
2. On client machine:
```bash
hoard setup remote --url https://hoard.example.com --token hoard_sk_xxx --codex
```

Advanced automation flow:
```bash
hoard setup remote --url https://hoard.example.com --admin-token hoard_admin_xxx --all
```
This creates one token per configured client type.

## Security Notes
- Do not expose plain HTTP to untrusted networks.
- Prefer narrow scopes per token and rotate compromised tokens.
- Keep `HOARD_SERVER_SECRET` out of shell history and committed files.
