"""Generate one trading day of realistic FinCore source files in the S3 layout.

    python scripts/generate_sample_data.py --out data/raw --date 2026-10-26 --trades 5000

Writes trades/dt=D/*.fix, market_data/dt=D/{equity,fixed_income,fx}.csv and
portfolio/dt=D/portfolio.jsonl. A small, fixed share of records is deliberately bad
(malformed FIX lines, invalid sides, an unpriced symbol, a NULL close price, broken JSON)
so the dead-letter path is exercised while the run still passes the quality gate.
"""

import argparse
import json
import logging
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

log = logging.getLogger("generate_sample_data")

# symbol -> (asset_class, currency, close)
INSTRUMENTS: dict[str, tuple[str, str, Decimal]] = {
    "AAPL": ("EQUITY", "USD", Decimal("228.52")),
    "MSFT": ("EQUITY", "USD", Decimal("417.13")),
    "JPM": ("EQUITY", "USD", Decimal("221.49")),
    "BRK.B": ("EQUITY", "USD", Decimal("458.05")),
    "VOD.L": ("EQUITY", "GBP", Decimal("0.7215")),
    "HSBA.L": ("EQUITY", "GBP", Decimal("7.1230")),
    "US10Y": ("FIXED_INCOME", "USD", Decimal("98.734")),
}
FX_RATES: dict[str, Decimal] = {
    "GBPUSD": Decimal("1.2965"), "EURUSD": Decimal("1.0812"), "USDJPY": Decimal("152.31"),
}
TRADEABLE = tuple(INSTRUMENTS)  # FX pairs live in FX_RATES, so every instrument is tradeable
PORTFOLIOS = tuple(f"PF{number:03d}" for number in range(1, 9))
CSV_HEADER = "symbol,asset_class,currency,close_price,as_of_date"
MALFORMED_PER_MILLE, BAD_SIDE_PER_MILLE, UNPRICED_PER_MILLE = 3, 2, 5
MAX_UNPRICED = 60
PRICE_STEP, QTY_RANGE, POSITION_RANGE = Decimal("0.0001"), (1, 1000), (100, 50_000)
MARKET_OPEN_UTC, SESSION_SECONDS = 13 * 3600 + 30 * 60, int(6.5 * 3600)


@dataclass(frozen=True)
class SampleSpec:
    trade_date: str
    trade_count: int = 5000
    seed: int = 42


def _price_near(rng: random.Random, close: Decimal, spread: float) -> Decimal:
    factor = Decimal(str(1 + rng.uniform(-spread, spread)))
    return (close * factor).quantize(PRICE_STEP, rounding=ROUND_HALF_UP)


def _fix_line(rng: random.Random, index: int, day: datetime, symbol: str, side: str) -> str:
    _, currency, close = INSTRUMENTS.get(symbol, ("EQUITY", "USD", Decimal("10")))
    executed = day + timedelta(seconds=MARKET_OPEN_UTC + rng.randrange(SESSION_SECONDS))
    shown_symbol = f" {symbol.lower()} " if index % 20 == 0 else symbol  # exercise normalization
    return (
        f"8=FIX.4.4|9=0|35=8|37=ORD{day:%Y%m%d}{index:07d}|11=CL{index:07d}"
        f"|1={rng.choice(PORTFOLIOS)}|55={shown_symbol}|54={side}|38={rng.randint(*QTY_RANGE)}"
        f"|44={_price_near(rng, close, 0.02)}|15={currency}|60={executed:%Y%m%d-%H:%M:%S}|10=000|"
    )


def _trade_lines(rng: random.Random, spec: SampleSpec) -> list[str]:
    count = spec.trade_count
    picks = rng.sample(range(count), k=min(count, 3 + count * 10 // 1000))
    malformed = set(picks[: max(1, count * MALFORMED_PER_MILLE // 1000)])
    bad_side = set(picks[len(malformed): len(malformed) + max(1, count * BAD_SIDE_PER_MILLE // 1000)])
    unpriced = set(rng.sample(range(count), k=min(MAX_UNPRICED, max(1, count * UNPRICED_PER_MILLE // 1000))))
    unpriced -= bad_side
    day = datetime.combine(date.fromisoformat(spec.trade_date), datetime.min.time())
    lines = []
    for index in range(count):
        if index in malformed:
            lines.append(f"CORRUPTED RECORD {index}|missing tags")
            continue
        side = "9" if index in bad_side else rng.choice(("1", "2"))
        symbol = "NOPRICE" if index in unpriced else rng.choice(TRADEABLE)
        lines.append(_fix_line(rng, index, day, symbol, side))
    return lines


def _market_files(trade_date: str) -> dict[str, list[str]]:
    rows: dict[str, list[str]] = {"equity.csv": [], "fixed_income.csv": [], "fx.csv": []}
    for symbol, (asset_class, currency, close) in INSTRUMENTS.items():
        file_name = "equity.csv" if asset_class == "EQUITY" else "fixed_income.csv"
        rows[file_name].append(f"{symbol},{asset_class},{currency},{close},{trade_date}")
    rows["equity.csv"].append(f"DELISTED,EQUITY,USD,,{trade_date}")  # NULL close -> dead letter
    for pair, rate in FX_RATES.items():
        rows["fx.csv"].append(f"{pair},FX,{pair[3:]},{rate},{trade_date}")
    return {name: [CSV_HEADER, *lines] for name, lines in rows.items()}


def _portfolio_lines(rng: random.Random, trade_date: str) -> list[str]:
    lines = []
    for portfolio_id in PORTFOLIOS:
        limits = {"max_position_value_usd": rng.choice((250_000, 1_000_000, 5_000_000)),
                  "max_var_usd": 150_000, "max_single_name_pct": 12.5}
        for symbol in rng.sample(TRADEABLE, k=4):
            _, currency, close = INSTRUMENTS[symbol]
            lines.append(json.dumps({
                "portfolio_id": portfolio_id, "symbol": symbol,
                "position_quantity": rng.randint(*POSITION_RANGE),
                "average_cost": float(_price_near(rng, close, 0.10)), "currency": currency,
                "as_of_date": trade_date,
                "metadata": {"portfolio_manager": f"PM-{portfolio_id[-1]}", "strategy": "core",
                             "risk_limits": limits},
            }))
    lines.append('{"portfolio_id": "PF999", "symbol": ')  # truncated export -> dead letter
    return lines


def _write(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def generate(spec: SampleSpec, out_dir: Path) -> list[Path]:
    date.fromisoformat(spec.trade_date)
    rng = random.Random(spec.seed)  # noqa: S311 - test data, not security
    partition = f"dt={spec.trade_date}"
    files = {out_dir / "trades" / partition / "trades_0001.fix": _trade_lines(rng, spec)}
    for name, lines in _market_files(spec.trade_date).items():
        files[out_dir / "market_data" / partition / name] = lines
    files[out_dir / "portfolio" / partition / "portfolio.jsonl"] = _portfolio_lines(rng, spec.trade_date)
    for path, lines in files.items():
        _write(path, lines)
    return list(files)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("data/raw"))
    parser.add_argument("--date", default="2026-10-26")
    parser.add_argument("--trades", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    for path in generate(SampleSpec(args.date, args.trades, args.seed), args.out):
        log.info("wrote %s", path)


if __name__ == "__main__":
    main()
