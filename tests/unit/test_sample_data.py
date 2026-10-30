from pathlib import Path

from generate_sample_data import SampleSpec, generate


def test_generates_all_three_sources_in_the_s3_layout(tmp_path: Path) -> None:
    generate(SampleSpec(trade_date="2026-10-26", trade_count=200, seed=7), tmp_path)
    assert (tmp_path / "trades" / "dt=2026-10-26" / "trades_0001.fix").exists()
    for name in ("equity.csv", "fixed_income.csv", "fx.csv"):
        assert (tmp_path / "market_data" / "dt=2026-10-26" / name).exists()
    assert (tmp_path / "portfolio" / "dt=2026-10-26" / "portfolio.jsonl").exists()


def test_bad_records_stay_below_the_quality_gate(tmp_path: Path) -> None:
    generate(SampleSpec(trade_date="2026-10-26", trade_count=5000, seed=7), tmp_path)
    lines = (tmp_path / "trades" / "dt=2026-10-26" / "trades_0001.fix").read_text().splitlines()
    malformed = [line for line in lines if not line.startswith("8=FIX.4.4")]
    bad_side = [line for line in lines if "|54=9|" in line]
    unpriced = [line for line in lines if "|55=NOPRICE|" in line]
    assert len(lines) == 5000
    assert 0 < (len(malformed) + len(bad_side)) / len(lines) < 0.02
    assert 0 < len(unpriced) < 100


def test_same_seed_is_reproducible(tmp_path: Path) -> None:
    generate(SampleSpec(trade_date="2026-10-26", trade_count=50, seed=1), tmp_path / "a")
    generate(SampleSpec(trade_date="2026-10-26", trade_count=50, seed=1), tmp_path / "b")
    path = Path("trades") / "dt=2026-10-26" / "trades_0001.fix"
    assert (tmp_path / "a" / path).read_text() == (tmp_path / "b" / path).read_text()
