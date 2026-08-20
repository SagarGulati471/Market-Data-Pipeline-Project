import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch
import matplotlib.patheffects as pe

# ── Palette ────────────────────────────────────────────────────────────────────
BG        = "#030e22"
BG_CARD   = "#071d3d"
BG_CARD2  = "#0a2550"
TEAL      = "#00c9b8"
TEAL_DIM  = "#007d72"
GREEN     = "#4ece6a"
GREEN_DIM = "#2d8a46"
BORDER    = "#1b3f6a"
WHITE     = "#ffffff"
LGRAY     = "#8aaac8"
GOLD      = "#f5a633"
PURPLE    = "#7c6af7"

W, H = 24, 11.5
fig = plt.figure(figsize=(W, H), facecolor=BG, dpi=150)
ax = fig.add_axes([0, 0, 1, 1], facecolor=BG)
ax.set_xlim(0, W)
ax.set_ylim(0, H)
ax.axis("off")


# ── Helpers ─────────────────────────────────────────────────────────────────────
def rbox(x, y, w, h, fc=BG_CARD, ec=BORDER, lw=1.2, radius=0.18, alpha=1, zorder=3):
    b = FancyBboxPatch((x, y), w, h,
                       boxstyle=f"round,pad={radius}",
                       facecolor=fc, edgecolor=ec,
                       linewidth=lw, alpha=alpha, zorder=zorder)
    ax.add_patch(b)


def txt(x, y, s, size=9, color=WHITE, weight="normal", ha="center", va="center", zorder=6):
    ax.text(x, y, s, fontsize=size, color=color, fontweight=weight,
            ha=ha, va=va, zorder=zorder,
            path_effects=[pe.withStroke(linewidth=1.5, foreground="black")])


def arrow(x1, y1, x2, y2, color=TEAL, lw=2.2):
    ax.annotate("",
                xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=color, lw=lw,
                                mutation_scale=14),
                zorder=5)


def dotline(x1, y1, x2, y2, color=LGRAY):
    ax.plot([x1, x2], [y1, y2], color=color, lw=1, ls="--", zorder=2, alpha=0.5)


# ── Background grid (subtle) ────────────────────────────────────────────────────
for xi in range(0, 25, 2):
    ax.plot([xi, xi], [0, H], color=BORDER, lw=0.3, alpha=0.2, zorder=0)
for yi in range(0, 14, 1):
    ax.plot([0, W], [yi, yi], color=BORDER, lw=0.3, alpha=0.2, zorder=0)

# ── Top banner ──────────────────────────────────────────────────────────────────
rbox(0.3, 9.75, 23.4, 1.55, fc=BG_CARD2, ec=TEAL, lw=2, radius=0.12)
txt(W / 2, 10.82, "REAL-TIME MARKET DATA PIPELINE  &  STRATEGY ENGINE",
    size=22, weight="bold", color=WHITE)
txt(W / 2, 10.17,
    "End-to-end system: live data collection  →  signal generation  →  risk-managed order execution",
    size=11, color=LGRAY)

# ── Section label: Pipeline Flow ────────────────────────────────────────────────
txt(W / 2, 9.42, "━━━━━━━━━━  PIPELINE FLOW  ━━━━━━━━━━",
    size=9.5, color=TEAL, weight="bold")


# ── Pipeline stages ─────────────────────────────────────────────────────────────
# Layout: 8 boxes across, centred vertically around y=8.6
stages = [
    ("DATA\nCOLLECTOR", "Finnhub\nWebSocket"),
    ("KAFKA\nBROKER",   "Message Bus\nKRaft mode"),
    ("NORMALIZER",      "Validate &\nstandardise"),
    ("CANDLE\nBUILDER", "1-min OHLCV\naggregation"),
    ("INDICATOR\nCALC", "RSI · MACD\nVWAP · EMA"),
    ("SIGNAL\nGEN",     "Strategy\nevaluation"),
    ("ORDER\nEXECUTOR", "Risk Mgr\n+ Paper Trade"),
    ("TIMESCALE\nDB",   "Hypertables\nPostgreSQL 16"),
]

BOX_W  = 2.45
BOX_H  = 1.55
GAP    = 0.46
START  = 0.55
Y_BOX  = 6.55

# colour overrides per box
COLORS = [
    (BG_CARD2, TEAL),      # data collector
    ("#0d3060", "#1e7fff"),  # kafka (blue)
    (BG_CARD2, TEAL),
    (BG_CARD2, TEAL),
    (BG_CARD2, TEAL),
    (BG_CARD2, TEAL),
    ("#0a2e18", GREEN),     # order executor (green accent)
    ("#1a1230", PURPLE),    # timescaledb (purple accent)
]

box_cx = []  # centre x of each box
for i, ((title, sub), (fc, ec)) in enumerate(zip(stages, COLORS)):
    x = START + i * (BOX_W + GAP)
    cx = x + BOX_W / 2
    box_cx.append(cx)
    rbox(x, Y_BOX, BOX_W, BOX_H, fc=fc, ec=ec, lw=2.0, radius=0.14)
    # coloured top bar
    rbox(x + 0.05, Y_BOX + BOX_H - 0.38, BOX_W - 0.1, 0.35,
         fc=ec, ec=ec, lw=0, radius=0.08, zorder=4)
    txt(cx, Y_BOX + BOX_H - 0.2, title, size=8.0, weight="bold",
        color=WHITE if fc != "#0d3060" else WHITE, zorder=7)
    txt(cx, Y_BOX + 0.52, sub, size=7.5, color=LGRAY)

# Arrows between boxes
for i in range(len(stages) - 1):
    x1 = START + i * (BOX_W + GAP) + BOX_W
    x2 = START + (i + 1) * (BOX_W + GAP)
    y  = Y_BOX + BOX_H / 2
    color = "#1e7fff" if i == 0 else (GREEN if i == 5 else TEAL)
    arrow(x1, y, x2, y, color=color)

# Kafka topic labels on arrows (small)
topics = [
    "market_data", "trades-\nnormalized", "candles", "indicators", "signals", "order-\nexecutor", ""
]
for i, t in enumerate(topics):
    if not t:
        continue
    mx = (START + i * (BOX_W + GAP) + BOX_W + START + (i + 1) * (BOX_W + GAP)) / 2
    txt(mx, Y_BOX + BOX_H / 2 + 0.38, t, size=6.0, color=LGRAY)


# ── Finnhub WebSocket source (above data collector) ─────────────────────────────
ws_cx = box_cx[0]
rbox(ws_cx - 1.15, Y_BOX + BOX_H + 0.38, 2.3, 0.62,
     fc="#081226", ec=TEAL, lw=1.5, radius=0.1)
txt(ws_cx, Y_BOX + BOX_H + 0.69, "Finnhub  WebSocket", size=8, color=TEAL, weight="bold")
arrow(ws_cx, Y_BOX + BOX_H + 0.38, ws_cx, Y_BOX + BOX_H, color=TEAL, lw=1.8)


# ── All-services-write-to-DB dashed line ────────────────────────────────────────
db_cx = box_cx[-1]
for i in range(1, len(stages) - 1):
    dotline(box_cx[i], Y_BOX, box_cx[i], Y_BOX - 0.45)
    if i == 1:
        dotline(box_cx[i], Y_BOX - 0.45, db_cx, Y_BOX - 0.45)
ax.annotate("", xy=(db_cx, Y_BOX), xytext=(db_cx, Y_BOX - 0.45),
            arrowprops=dict(arrowstyle="-|>", color=PURPLE, lw=1.4,
                            mutation_scale=10, linestyle="dashed"),
            zorder=5)
txt(box_cx[3], Y_BOX - 0.62, "all services persist to TimescaleDB",
    size=6.5, color=LGRAY)


# ── Feature callout boxes ────────────────────────────────────────────────────────
features = [
    (TEAL,   "Dead-Letter Topics",      "Failed messages\nrouted to DLT"),
    (GOLD,   "Async Python",            "asyncio · aiokafka\nasyncpg"),
    (GREEN,  "Risk Manager",            "10 pre-trade checks\nbefore every order"),
    (GREEN,  "Paper Trading",           "Simulated fills\nwith slippage model"),
    ("#ff6b6b", "Auto Square-Off",      "Closes intraday\npositions at 3:30 PM"),
    (PURPLE, "Startup Reconciliation",  "Replays DB orders\non container restart"),
]

FC_W   = 3.4
FC_H   = 1.1
FC_Y   = 4.35
FC_GAP = 0.38
fc_total = len(features) * FC_W + (len(features) - 1) * FC_GAP
fc_start = (W - fc_total) / 2

txt(W / 2, FC_Y + FC_H + 0.3,
    "━━━━━━━━━━  KEY FEATURES  ━━━━━━━━━━",
    size=9.5, color=TEAL, weight="bold")

for i, (color, title, body) in enumerate(features):
    x = fc_start + i * (FC_W + FC_GAP)
    cx = x + FC_W / 2
    rbox(x, FC_Y, FC_W, FC_H, fc=BG_CARD, ec=color, lw=1.8, radius=0.12)
    rbox(x, FC_Y, 0.12, FC_H, fc=color, ec=color, lw=0, radius=0.06, zorder=4)
    txt(cx + 0.06, FC_Y + FC_H - 0.3, title, size=8.5, weight="bold", color=color)
    txt(cx + 0.06, FC_Y + 0.38, body, size=7.5, color=LGRAY)


# ── Tech stack strip ─────────────────────────────────────────────────────────────
TS_Y = 3.28
rbox(0.3, TS_Y, 23.4, 0.72, fc="#040f20", ec=BORDER, lw=1, radius=0.1)
tech_items = [
    ("Python 3.14", TEAL),
    ("Apache Kafka 4.2.0  (KRaft)", "#1e7fff"),
    ("TimescaleDB 2.27.1  (PG16)", PURPLE),
    ("Docker Compose", GOLD),
    ("Pydantic v2", GREEN),
    ("asyncio · aiokafka · asyncpg", "#ff9a6b"),
]
n = len(tech_items)
xs = [0.3 + (i + 0.5) * 23.4 / n for i in range(n)]
for x, (label, color) in zip(xs, tech_items):
    txt(x, TS_Y + 0.36, label, size=8.2, color=color, weight="bold")


# ── Bottom summary ──────────────────────────────────────────────────────────────
rbox(0.3, 2.08, 23.4, 1.0, fc=BG_CARD2, ec=BORDER, lw=1, radius=0.12)
txt(W / 2, 2.76,
    "A fully containerised, event-driven pipeline that streams live US equity ticks from Finnhub, "
    "builds OHLCV candles,",
    size=9, color=LGRAY)
txt(W / 2, 2.33,
    "computes technical indicators, generates trading signals, and executes paper orders — "
    "gated by a 10-check risk manager with automatic intraday square-off.",
    size=9, color=LGRAY)

# "SUMMARY" badge
rbox(0.4, 2.18, 1.5, 0.78, fc=TEAL_DIM, ec=TEAL, lw=1.5, radius=0.12, zorder=5)
txt(1.15, 2.57, "SUMMARY", size=8.5, weight="bold", color=WHITE, zorder=6)

# ── Footer ──────────────────────────────────────────────────────────────────────
txt(W / 2, 1.72, "github.com/SagarGulati471",
    size=8, color=LGRAY)

# ── Save ────────────────────────────────────────────────────────────────────────
out = "Project-visuals/market-data-pipeline-v2.png"
plt.tight_layout(pad=0)
fig.savefig(out, dpi=150, bbox_inches="tight",
            facecolor=BG, edgecolor="none")
print(f"Saved → {out}")
