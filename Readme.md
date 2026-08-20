# Market-Data-Pipeline-and-Strategy-Engine

A real-time market data pipeline that collects live trade data, processes it through multiple stages, generates buy/sell signals based on technical indicators, and executes paper trades with built-in risk management — all running as Docker containers connected through Apache Kafka.

The goal of this project was to build something production-grade from scratch: event-driven architecture, async Python throughout, proper containerization, and a clean separation between data collection, processing, signal generation, and order execution.

---

## Architecture

![Architecture Diagram](https://github.com/user-attachments/assets/87cb23d4-630b-47b6-aa6d-37e675a0351b)

**Full diagram:** https://app.diagrams.net/#G1XzVCH1INkYbS2aGT_wJ4cgXVnr5ShD6k

---

## How Data Flows Through the System

```
┌──────────────────┐
│ Finnhub WebSocket│
└────────┬─────────┘
         │
         ▼
┌────────────────┐
│ Data Collector │
└────────┬───────┘
         │
         ▼
╔═════════════════════════════════════════════════════════════════════════════╗
║                              Apache Kafka                                   ║
║                                                                             ║
║  market_data → trades-normalized → candles → indicators → signals → orders  ║
╚═══════╤══════════════╤════════════╤══════════════╤════════════╤═════════════╝
        │              │            │              │            │
        ▼              ▼            ▼              ▼            ▼
 ┌────────────┐  ┌─────────────┐  ┌───────────┐  ┌────────────┐  ┌──────────────┐
 │ Normalizer │→ │Candle Builder →│ Indicators│→ │  Signals   │→ │Order Executor│
 └────────────┘  └─────────────┘  └───────────┘  └────────────┘  └──────┬───────┘
                                                                         │
                                                                         ▼
                                                              ┌──────────────────┐
                                                              │    TimescaleDB   │
                                                              └──────────────────┘
```

Each stage is a separate Docker container. They all run independently and communicate only through Kafka topics — no direct service-to-service calls.

---

## Services

| Service | What it does |
|---|---|
| `data_collector` | Connects to the Finnhub WebSocket API and streams raw tick data for configured stock symbols into Kafka |
| `normalizer` | Consumes raw ticks, validates and transforms them into a consistent internal format, persists to TimescaleDB |
| `candle_builder` | Aggregates normalized trades into OHLCV candles (1-minute), persists and forwards to next topic |
| `indicator_calculator` | Computes technical indicators (RSI, MACD, etc.) from candles, persists and publishes to indicators topic |
| `signal_generator` | Evaluates indicator values against strategy rules, generates BUY/SELL/HOLD signals with confidence scores |
| `order_executor` | Consumes signals, runs them through the risk manager, and places orders via the paper trading adapter |

**Infrastructure:**

| Service | What it does |
|---|---|
| `kafka` | Apache Kafka 4.2.0 in KRaft mode (no ZooKeeper) — the message bus that connects all services |
| `timescale` | TimescaleDB (PostgreSQL 16 + time-series extension) — stores all trade, candle, indicator, signal, and order data |
| `kafka-ui` | Kafbat Kafka UI — browser-based tool to inspect topics, messages, and consumer groups |
| `pgadmin` | PGAdmin 4 — browser-based PostgreSQL client |

---

## Tech Stack

- **Language:** Python 3.14
- **Async:** `asyncio` + `aiokafka` + `asyncpg`
- **Data validation:** Pydantic v2
- **Config management:** Pydantic BaseModel + `python-dotenv`
- **Message broker:** Apache Kafka 4.2.0 (KRaft mode)
- **Database:** TimescaleDB 2.27.1 (PostgreSQL 16)
- **Containerization:** Docker + Docker Compose with profiles

---

## Order Executor — Risk Management

The order executor is the most complex service. Before any order is placed, the risk manager runs the signal through a set of checks:

- Signal is not stale (configurable max age in seconds)
- No conflicting open position exists for the symbol
- Daily loss limit has not been breached
- Per-trade capital limit is respected
- Max open positions limit is not exceeded
- Max position size per symbol is not exceeded
- Order rate limit (orders per minute) is not exceeded
- Close price is available for capital calculation
- No duplicate order for the same signal

If any check fails, the signal is rejected and logged with the specific reason. If all checks pass, the order goes to the paper adapter, which simulates a fill with a small random slippage and writes it to the database.

On startup, the service reads today's filled orders from the database and reconciles its in-memory position state — so a container restart doesn't lose track of open positions.

There is also a background task that fires at 3:30 PM ET to auto square-off any open intraday positions.

---

## Project Structure

```
Project-Root/
├── config/                  # Central config loaded from environment variables
├── consumers/
│   ├── normalizer/          # Stage 1: raw → normalized trades
│   ├── candle_builder/      # Stage 2: trades → OHLCV candles
│   ├── indicator/           # Stage 3: candles → technical indicators
│   └── signal_generator/    # Stage 4: indicators → trading signals
├── data_collectors/
│   └── Finnhub/             # WebSocket client for Finnhub live data
├── messaging/
│   └── kafka_service/       # Kafka producer/consumer wrappers
├── order_executor/
│   ├── broker_adapter/
│   │   └── paper_adapter/   # Simulates order fills with slippage
│   ├── order_manager/       # Orchestrates risk check → place → record → publish
│   ├── risk_manager/        # 10 pre-trade risk checks
│   ├── consumer.py          # Entry point, startup reconciliation, square-off task
│   ├── models.py            # Order, RiskConfig, CurrentPositionSize, enums
│   └── order_repository.py  # Shared DB queries for orders
├── storage/
│   └── timescaledb_deployment/
│       ├── migrations/      # SQL migration files (run automatically on first container start)
│       └── db_wrapper.py    # asyncpg connection pool management
├── utils/
│   └── logger.py            # Structured logging setup
├── Dockerfile
├── docker-compose.yml
├── docker-compose.prod.yml
└── requirements.txt
```

---

## Getting Started

### Prerequisites

- Docker Desktop (or Docker Engine + Docker Compose)
- A free [Finnhub](https://finnhub.io) account for the API token

### 1. Clone the repository

```bash
git clone <repo-url>
cd Market-Data-Pipeline-Project/Project-Root
```

### 2. Set up your environment file

```bash
cp .env.example .env
```

Open `.env` and fill in the required values. The only mandatory one to change before running is `FINNHUB_API_TOKEN`. Everything else has sensible defaults.

### 3. Build the Docker image

All app services share a single Docker image. Build it once:

```bash
docker compose build
```

### 4. Start the full stack

```bash
# Start infrastructure (Kafka + TimescaleDB) — these start without a profile
docker compose up kafka timescale -d

# Wait for both to be healthy, then start all app services
docker compose --profile app_containers up -d
```

To also start the UI tools (Kafka UI + PGAdmin):

```bash
docker compose --profile infrastructure_containers up -d
```

Or bring everything up at once:

```bash
docker compose --profile "*" up -d
```

### 5. Verify it's running

```bash
docker compose ps
docker compose logs -f data_collector
```

If you see trade data being logged, the pipeline is up. Once enough candles accumulate, the signal generator will start producing signals and the order executor will start placing paper orders.

---

## Useful Commands

```bash
# Tail logs for a specific service
docker compose logs -f <service_name>

# Stop all containers but keep volumes (data is preserved)
docker compose --profile "*" down

# Stop and delete all data (full reset)
docker compose --profile "*" down -v

# Access the database directly
docker exec -it timescale psql -U <DB_USER> -d <DB_NAME>
```

**Kafka UI:** http://localhost:8080  
**PGAdmin:** http://localhost:5050 (login with `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD` from your `.env`)

---

## Environment Variables

See `.env.example` for the full list with descriptions. Key variables:

| Variable | Default | Description |
|---|---|---|
| `FINNHUB_API_TOKEN` | — | Your Finnhub API token (required) |
| `FINNHUB_STOCK_SYMBOLS` | `AAPL,AMZN,CSCO` | Comma-separated symbols to stream |
| `IS_PAPER_TRADING` | `True` | Set to `False` when a live broker adapter is connected |
| `MAX_CAPITAL_PER_TRADE` | `1000.0` | Max USD capital per order |
| `MAX_DAILY_LOSS` | `5000.0` | Daily loss limit before all new orders are blocked |
| `MAX_OPEN_POSITIONS` | `10` | Max number of concurrent open positions |
| `MAX_ORDERS_PER_MINUTE` | `5` | Rate limiter for order placement |
| `SIGNAL_MAX_AGE_SECONDS` | `60` | Signals older than this are rejected as stale |
| `LOG_LEVEL` | `INFO` | Application log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

---

## Known Limitations / Roadmap

- **Paper trading only** — a `FyersAdapter` for live Indian market trading is partially designed but not yet implemented. The adapter interface is in place; swapping `IS_PAPER_TRADING=False` is all that will be needed once it's built.
- **Square-off price is approximate** — the intraday auto square-off fires at 3:30 PM ET but uses a placeholder price. A last-known price cache or a REST price fetch is the next step here.
- **Single-node Kafka** — fine for development and moderate data volumes. Production would need a multi-broker cluster with replication.
- **In-memory position state** — the order executor tracks open positions in memory. A Redis layer is the planned upgrade path for multi-instance deployments.

---

## Running Locally (without Docker)

```bash
cd Project-Root
python -m venv venv_3.14
source venv_3.14/bin/activate
pip install -r requirements.txt

# Start only the infrastructure containers
docker compose up kafka timescale -d

# Then run any service directly
python -m data_collectors.Finnhub.websocket
python -m consumers.normalizer.consumer
python -m order_executor.consumer
```

---

## Author

Sagar Gulati
