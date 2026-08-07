import datetime
import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).parents[2]))

from etl.transformation.silver.sec_company_facts_padded import _pad_series


def test_pad_series_drops_same_day_superseded_entry_without_null_reference_date():
    """Two entries filed on the same date must not produce a null
    reference_date: the earlier one (by end_col) is instantly superseded by
    the later one filed the same day, so it should be dropped outright
    rather than leak through as a zero-length, then null-exploded, range."""

    lf = pl.DataFrame(
        {
            "cik": [1, 1, 1],
            "end": [
                datetime.date(2020, 1, 1),  # superseded same-day entry
                datetime.date(2020, 4, 1),  # surviving same-day entry
                datetime.date(2020, 7, 1),  # most recent entry, padded to `today`
            ],
            "filed": [
                datetime.date(2020, 2, 1),
                datetime.date(2020, 2, 1),
                datetime.date(2020, 3, 1),
            ],
            "val": [10, 20, 30],
        }
    ).lazy()

    result = _pad_series(
        lf, end_col="end", filed_col="filed", today=datetime.date(2020, 4, 1)
    ).collect()

    assert result.filter(pl.col("reference_date").is_null()).height == 0
    assert 10 not in result["val"].to_list()

    surviving = result.filter(pl.col("val") == 20).sort("reference_date")
    assert surviving["reference_date"].min() == datetime.date(2020, 2, 1)
    assert surviving["reference_date"].max() == datetime.date(2020, 2, 29)

    most_recent = result.filter(pl.col("val") == 30).sort("reference_date")
    assert most_recent["reference_date"].min() == datetime.date(2020, 3, 1)
    assert most_recent["reference_date"].max() == datetime.date(2020, 4, 1)
