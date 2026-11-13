import json
import math
import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest
import yaml

sys.path.append(str(Path(__file__).parents[3]))
from analysis.evaluate import cli, experiment_log, tracking

PICK = "tests.analysis.evaluate.sample_strategies:Pick"
PEEKER = "tests.analysis.evaluate.sample_strategies:Peeker"
NEEDS_RSI = "tests.analysis.evaluate.sample_strategies:NeedsRsi"
HOLDOUT = date(2022, 7, 1)


def business_days(start: date, end: date) -> list[date]:
    days, d = [], start
    while d < end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


@pytest.fixture
def config(tmp_path: Path) -> Path:
    """A config pointing at a synthetic 3-year data cache in tmp_path."""
    rows = []
    for k, symbol in enumerate("ABC"):
        price = 100.0
        for i, d in enumerate(business_days(date(2020, 1, 1), date(2023, 1, 1))):
            open_ = price
            price *= 1 + 0.0004 * (k + 1) + 0.01 * math.sin(i * (k + 2))
            rows.append(
                {
                    "timeframe": d,
                    "symbol": symbol,
                    "open": open_,
                    "high": max(open_, price),
                    "low": min(open_, price),
                    "close": price,
                    "volume": 1000.0,
                }
            )
    cache = tmp_path / "cache.parquet"
    pl.DataFrame(rows).write_parquet(cache)
    (tmp_path / "cache.meta.json").write_text(json.dumps({"source": "synthetic"}))
    path = tmp_path / "evaluation.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "holdout_start": HOLDOUT,
                "data_cache": str(cache),
                "dataplatform_root": str(tmp_path),
                "risk_free_region": None,
                "initial_cash": 10000,
                "commission_bps": 5,
                "slippage_bps": 5,
                "max_leverage": 1.0,
                "experiment_log": str(tmp_path / "log.jsonl"),
                "mlflow_experiment": "test",
            }
        )
    )
    return path


@pytest.fixture(autouse=True)
def no_real_mlflow(monkeypatch):
    calls = []

    def fake_log_run(experiment, report, result):
        calls.append(report["run_id"])
        return "fake-mlflow-id"

    monkeypatch.setattr(tracking, "log_run", fake_log_run)
    return calls


def run(capsys, config: Path, *args: str) -> tuple[int, dict]:
    code = cli.main(["--config", str(config), *args])
    out = capsys.readouterr().out
    return code, json.loads(out)  # stdout must be exactly one JSON document


class TestRun:
    def test_backtest_report(self, capsys, config, no_real_mlflow):
        code, report = run(capsys, config, "run", PICK, "--params", '{"symbol": "C"}')
        assert code == 0 and report["status"] == "ok"
        assert report["strategy"]["params"] == {"symbol": "C", "exposure": 1.0}
        assert report["guardrails"]["lookahead"]["passed"]
        assert set(report["metrics"]) >= {
            "sharpe",
            "cagr",
            "total_return",
            "max_drawdown",
        }
        assert report["activity"]["trades"] > 0
        assert report["mlflow_run_id"] == "fake-mlflow-id"
        assert no_real_mlflow == [report["run_id"]]

    def test_no_mlflow_flag(self, capsys, config, no_real_mlflow):
        code, report = run(capsys, config, "run", PICK, "--no-mlflow")
        assert code == 0 and report["mlflow_run_id"] is None
        assert no_real_mlflow == []

    def test_mlflow_failure_keeps_the_evaluation(self, capsys, config, monkeypatch):
        def down(*args):
            raise ConnectionError("tracking server down")

        monkeypatch.setattr(tracking, "log_run", down)
        code, report = run(capsys, config, "run", PICK)
        assert code == 0 and report["metrics"] is not None
        assert "tracking server down" in report["mlflow_error"]

    def test_report_written_to_file(self, capsys, config, tmp_path):
        out = tmp_path / "report.json"
        _, report = run(
            capsys, config, "run", PICK, "--no-mlflow", "--output", str(out)
        )
        assert json.loads(out.read_text())["run_id"] == report["run_id"]

    def test_lookahead_is_caught(self, capsys, config, no_real_mlflow):
        code, report = run(capsys, config, "run", PEEKER)
        assert code == cli.EXIT_GUARDRAIL
        assert report["status"] == "guardrail_failed"
        assert report["metrics"] is None
        assert report["guardrails"]["lookahead"]["failures"]
        assert no_real_mlflow == []  # nothing to track
        logged = experiment_log.read(
            Path(yaml.safe_load(config.read_text())["experiment_log"])
        )
        assert logged[-1]["status"] == "guardrail_failed"

    def test_walk_forward(self, capsys, config):
        code, report = run(
            capsys,
            config,
            "run",
            PICK,
            "--no-mlflow",
            "--protocol",
            "walk_forward",
            "--params",
            '{"exposure": 1.0}',
            "--param-grid",
            '{"symbol": ["A", "C"]}',
            "--train",
            "6mo",
            "--test",
            "3mo",
            "--warmup",
            "1mo",
        )
        assert code == 0
        wf = report["walk_forward"]
        assert wf["candidates_per_fold"] == 2
        assert all(f["params"]["exposure"] == 1.0 for f in wf["folds"])
        assert {f["params"]["symbol"] for f in wf["folds"]} <= {"A", "C"}
        assert "is_oos_gap" in report["metrics"]
        assert report["experiment_log"]["variants_tested"] == 2


class TestHoldout:
    def test_data_ends_before_the_holdout(self, capsys, config):
        _, report = run(capsys, config, "run", PICK, "--no-mlflow")
        assert report["window"]["end"] == HOLDOUT.isoformat()
        assert date.fromisoformat(report["window"]["last_day"]) < HOLDOUT

    @pytest.mark.parametrize(
        "args",
        [
            ["--end", "2022-12-01"],
            ["--start", "2022-08-01"],
            ["--holdout", "--protocol", "walk_forward"],
        ],
    )
    def test_reaching_into_the_holdout_is_refused(self, capsys, config, args):
        code, report = run(capsys, config, "run", PICK, "--no-mlflow", *args)
        assert code == cli.EXIT_INVALID and report["status"] == "error"

    def test_explicit_holdout_backtest(self, capsys, config):
        code, report = run(capsys, config, "run", PICK, "--no-mlflow", "--holdout")
        assert code == 0
        assert date.fromisoformat(report["window"]["first_evaluated_day"]) >= HOLDOUT
        assert report["window"]["holdout"] is True


class TestInvalidInput:
    @pytest.mark.parametrize(
        "args, message",
        [
            (["run", "NoSuchStrategy"], "Unknown strategy"),
            (["run", PICK, "--params", '{"symbol": "Z"}'], "outside its domain"),
            (["run", PICK, "--params", '{"nope": 1}'], "Unknown parameter"),
            (["run", PICK, "--params", "{not json"], "not valid JSON"),
            (["run", PICK, "--start", "01/02/2021"], "YYYY-MM-DD"),
            (["run", NEEDS_RSI], "Unknown data column"),
            (
                [
                    "run",
                    PICK,
                    "--protocol",
                    "walk_forward",
                    "--param-grid",
                    '{"symbol": ["Z"]}',
                ],
                "invalid for 'symbol'",
            ),
        ],
    )
    def test_errors_are_json(self, capsys, config, args, message):
        code, report = run(capsys, config, *[*args, "--no-mlflow"])
        assert code == cli.EXIT_INVALID
        assert report["status"] == "error" and message in report["error"]


class TestSurvivorshipAndUniverse:
    def test_unmeasurable_survivorship_is_still_warned(self, capsys, config):
        # the synthetic setup has no unfiltered prices to measure
        _, report = run(capsys, config, "run", PICK, "--no-mlflow")
        assert report["data"]["survivorship"] is None
        assert any("could not be measured" in w for w in report["warnings"])

    def test_measured_survivorship_reaches_the_report(self, capsys, config, tmp_path):
        measured = {"survivors_only": True, "warnings": ["survivorship bias: measured"]}
        (tmp_path / "cache.meta.json").write_text(
            json.dumps({"survivorship": measured})
        )
        _, report = run(capsys, config, "run", PICK, "--no-mlflow")
        assert report["data"]["survivorship"] == measured
        assert "survivorship bias: measured" in report["warnings"]

    def test_universe_column_in_the_cache_is_enforced(self, capsys, config, tmp_path):
        cache = tmp_path / "cache.parquet"
        data = pl.read_parquet(cache).with_columns(
            (pl.col("symbol") != "C").alias("in_universe")
        )
        data.write_parquet(cache)
        code, report = run(
            capsys, config, "run", PICK, "--no-mlflow", "--params", '{"symbol": "C"}'
        )
        assert code == cli.EXIT_INVALID and "universe" in report["error"]
        code, _ = run(
            capsys, config, "run", PICK, "--no-mlflow", "--params", '{"symbol": "A"}'
        )
        assert code == 0


class TestListAndHistory:
    def test_list_finds_the_package_strategies(self, capsys, config):
        code, listing = run(capsys, config, "list")
        assert code == 0
        momentum = next(s for s in listing["strategies"] if s["name"] == "TopNMomentum")
        assert momentum["params"]["top_n"] == {
            "default": 20,
            "low": 10,
            "high": 30,
            "step": 10,
        }
        assert momentum["description"]

    def test_history_counts_variants(self, capsys, config):
        for params in ['{"symbol": "A"}', '{"symbol": "B"}', '{"symbol": "A"}']:
            run(capsys, config, "run", PICK, "--no-mlflow", "--params", params)
        code, history = run(capsys, config, "history", "Pick")
        assert code == 0
        assert history["evaluations"] == 3
        assert history["variants_tested"] == 2
        assert [e["params"]["symbol"] for e in history["entries"]] == ["A", "B", "A"]


ROBUST_FAST = ["--n-random", "5", "--bootstrap-samples", "50", "--n-paths", "2"]


class TestRobustness:
    def test_full_robustness_report(self, capsys, config, no_real_mlflow):
        code, report = run(
            capsys, config, "run", PICK, "--protocol", "robustness", *ROBUST_FAST
        )
        assert code == 0 and report["status"] == "ok"
        rob = report["robustness"]
        assert set(rob) == set(cli.ROBUSTNESS_CHECKS)
        assert rob["bootstrap"]["n"] == 50
        assert rob["random_baseline"]["n"] == 5
        assert rob["random_walk"]["n_paths"] == 2
        # Pick's grid: 3 symbols x 2 exposures, the defaults are one of them
        assert rob["sensitivity"]["candidates"] == 6
        assert rob["sensitivity"]["evaluated_params_rank"] in range(1, 7)
        assert report["experiment_log"]["variants_tested"] == 6
        dsr = rob["deflated_sharpe"]
        assert dsr["n_trials"] == 6 and dsr["trials_sharpe_variance"] > 0
        assert 0 <= dsr["deflated_sharpe"] <= 1
        # headline numbers are flat metrics, so mlflow can chart them
        assert {
            "deflated_sharpe",
            "random_baseline_percentile",
            "bootstrap_sharpe_low",
        } <= set(report["metrics"])
        assert no_real_mlflow == [report["run_id"]]

    def test_selected_checks_only(self, capsys, config):
        code, report = run(
            capsys,
            config,
            "run",
            PICK,
            "--no-mlflow",
            "--protocol",
            "robustness",
            "--checks",
            "bootstrap,random_walk",
            *ROBUST_FAST,
        )
        assert code == 0
        assert set(report["robustness"]) == {"bootstrap", "random_walk"}
        assert report["experiment_log"]["variants_tested"] == 1

    def test_deflated_sharpe_uses_earlier_trials_without_a_grid(self, capsys, config):
        for symbol in "ABC":
            run(
                capsys,
                config,
                "run",
                PICK,
                "--no-mlflow",
                "--params",
                f'{{"symbol": "{symbol}"}}',
            )
        _, report = run(
            capsys,
            config,
            "run",
            PICK,
            "--no-mlflow",
            "--protocol",
            "robustness",
            "--checks",
            "deflated_sharpe",
        )
        dsr = report["robustness"]["deflated_sharpe"]
        assert dsr["n_trials"] == 3  # A (default params) was already one of them
        assert dsr["trials_sharpe_variance"] > 0

    @pytest.mark.parametrize(
        "args", [["--checks", "nope"], ["--checks", ""], ["--holdout"]]
    )
    def test_invalid(self, capsys, config, args):
        code, report = run(
            capsys,
            config,
            "run",
            PICK,
            "--no-mlflow",
            "--protocol",
            "robustness",
            *args,
        )
        assert code == cli.EXIT_INVALID and report["status"] == "error"
