import pandas as pd
import numpy as np
import pytest
import sys
from pathlib import Path

# Add the src directory to sys.path to allow for imports
sys.path.append(str(Path(__file__).parents[2]))
from analysis.utils import label_returns_dynamic


# ---------------------------------------------------------
# Helper: Build a tiny example DF
# ---------------------------------------------------------
def make_df():
    return pd.DataFrame(
        {
            "symbol": ["A"] * 6,
            "timeframe": pd.date_range("2020-01-01", periods=6),
            "open": [100, 102, 101, 103, 104, 106],
        }
    )


def test_two_class_behavior():
    """test 2-class up/down behavior"""
    df = label_returns_dynamic(make_df(), thresholds=[0], steps=1)
    labels = df["label"].dropna().unique()

    # expected 2 classes: 0 (down) and 1 (up)
    assert set(labels).issubset({0, 1})

    # day0 to day1 goes up by 2
    assert df.loc[0, "label"] == 1
    # day1 to day2 goes down by 1
    assert df.loc[1, "label"] == 0


def test_symmetric_thresholds():
    """test symmetric multi-class behavior with positive thresholds"""
    thresholds = [0.01, 0.03]
    df = label_returns_dynamic(make_df(), thresholds=thresholds, steps=1)

    labels = df["label"].dropna().unique()
    assert labels.min() >= 0
    assert labels.max() < 5


def test_thresholds_include_zero():
    """test thresholds that include 0 create micro up/down bins"""
    thresholds = [0, 0.01]
    df = label_returns_dynamic(make_df(), thresholds=thresholds, steps=1)
    labels = df["label"].dropna().unique()

    assert labels.min() >= 0
    assert labels.max() < 4

    # Explicit small-up/small-down classification:
    # Movement is slight between day1→day2 (102→101: -0.0098)
    # Should fall into class 1 (small_down) in this config
    assert df.loc[1, "label"] in {1}


def test_nan_tail_behavior():
    """test labels are NaN only in tail where no future prices exist"""
    df = make_df()

    out = label_returns_dynamic(df, thresholds=[0.01], steps=2)

    # steps=2 means last two rows cannot be labeled
    nan_rows = out["label"].isna().sum()
    assert nan_rows == 2


def test_custom_labels():
    """test custom labels matching correct number of bins"""
    df = make_df()

    thresholds = [0.01]
    # Expected bins: [-inf, -0.01, 0.01, inf] -> 3 classes
    custom = ["down", "flat", "up"]
    out = label_returns_dynamic(
        df, thresholds=thresholds, steps=1, custom_labels=custom
    )
    assert set(out["label"].dropna().unique()).issubset(set(custom))

    custom = ["one", "two"]  # wrong length
    with pytest.raises(ValueError):
        label_returns_dynamic(df, thresholds=thresholds, steps=1, custom_labels=custom)


def test_label_count_matches_thresholds():
    """ensure label range matches theoretical class count"""
    df = make_df()

    thresholds = [0.01, 0.02, 0.05]
    out = label_returns_dynamic(df, thresholds=thresholds, steps=1)

    pos = sorted(thresholds)
    nega = [-t for t in reversed(pos) if t != 0]
    num_classes = len(nega) + len(pos)  # interior boundaries
    # bins = num_classes + 1
    labels = out["label"].dropna().unique()

    assert labels.min() >= 0
    assert labels.max() <= num_classes
