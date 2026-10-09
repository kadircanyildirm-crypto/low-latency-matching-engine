# Deploying the public demo

The demo is three containers: the exchange (the gateway, with the engine, its journal and
the web page), the bots that make its market, and [Caddy](https://caddyserver.com) in
front, which gets an HTTPS certificate from Let's Encrypt and passes the page and its
WebSocket through. Paper money only.

## What it needs

- A Linux server. One virtual CPU and 1 GB of memory are enough for the demo; the first
  build needs more time on such a machine, not more memory, with `JOBS=1`. Oracle Cloud's
  always-free virtual machines or a small Hetzner one do.
- Docker with the compose plugin.
- A domain or subdomain whose A record points at the server, and ports 80 and 443 open.
  Without one, `DOMAIN=:80` serves plain HTTP on port 80.

## Starting it

```sh
git clone https://github.com/kadircanyildirm-crypto/low-latency-matching-engine.git
cd low-latency-matching-engine
sh deploy/make-accounts.sh                 # the bots' accounts, with random tokens
DOMAIN=demo.example.com docker compose -f deploy/compose.yaml up -d --build
```

The page is then on `https://demo.example.com`. Each visitor gets a paper account with
$100,000 and 1,000 shares on the first visit.

## Running it

- **Restarts.** The exchange's directory is a volume, `exchange-data`: the journal,
  snapshots, checkpoints and the visitors' accounts survive a restart of the container or
  the server, and the market carries on where it stopped. The containers restart by
  themselves.
- **Logs.** `docker compose -f deploy/compose.yaml logs -f exchange`.
- **Upgrades.** `git pull`, then the same `up -d --build`. A release that raises the
  matching rules version (`orderbook::RULES_VERSION`) cannot replay the old journal, and
  the exchange refuses to start on it; for a paper market, starting over is simplest:
  `docker compose -f deploy/compose.yaml down` and `docker volume rm deploy_exchange-data`.
- **What is exposed.** Only Caddy's ports 80 and 443. The binary protocol's port, 9000,
  stays on the compose network for the bots.
