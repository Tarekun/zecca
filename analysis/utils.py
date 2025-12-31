import pandas as pd
import numpy as np


def label_returns_dynamic(
    df: pd.DataFrame,
    thresholds: list,
    steps: int,
    price_col: str = "open",
    custom_labels: list | None = None,
) -> pd.DataFrame:
    """
    Compute future-return labels for a stock time series using dynamic,
    symmetric threshold bins.
    The function aligns rows by symbol, computes the future price after a
    specified lookahead (`steps`), derives the percentage return, and assigns
    each row to a label based on user-provided return thresholds.

    Threshold interpretation:
        - The list `thresholds` should contain non-negative values.
        - Thresholds are made symmetric by reflecting them around zero.
        - For example:
              thresholds = [0.01, 0.03]
              → bins = [-inf, -0.03, -0.01, 0.01, 0.03, inf] (5 classes)

              thresholds = [0]   (binary classification)
              → bins = [-inf, 0, inf] (2 classes: down / up)

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe containing at least:
        - a `symbol` column identifying the asset
        - a `timeframe` column used for chronological ordering
        - a price column specified by `price_col`

    thresholds : list
        List of positive thresholds defining bin boundaries.
        The number of classes will be:
            n_classes = 2 * len(thresholds_nonzero) + 1
        unless 0 is included, which collapses the "near-zero" region into one class.

    steps : int
        Number of future rows to look ahead when computing returns.

    price_col : str, default "open"
        Column containing the price used to compute returns.

    custom_labels : list or None, optional
        A list of custom label names (strings or integers).
        Must have exactly `len(bin_edges) - 1` elements.
        If None, labels are returned as integer class IDs starting from 0.

    Returns
    -------
    pd.DataFrame
        A copy of the input dataframe with the following added columns:
        - `<price_col>_future_<steps>` : the future price
        - `future_return_<steps>`     : computed percentage return
        - `label`                      : assigned class label

    Raises
    ------
    ValueError
        If `custom_labels` is provided but its length does not match
        the computed number of classes.

    Notes
    -----
    - Bins are computed as:
          bin_edges = [-inf] + negative_thresholds + positive_thresholds + [inf]
    - Negative thresholds are constructed as the reversed negation of positives,
      excluding zero to avoid creating duplicate zero boundaries.

    Examples
    --------
    >>> df = pd.DataFrame({
    ...     "symbol": ["A", "A", "A"],
    ...     "timeframe": [1, 2, 3],
    ...     "open": [100, 102, 101],
    ... })
    >>> label_returns_dynamic(df, thresholds=[0.01, 0.03], steps=1)
    """
    posi = sorted(thresholds)
    # remove 0 from negative side so we dont duplicate it
    nega = [-t for t in reversed(posi) if t != 0]
    bin_edges = [-np.inf] + nega + posi + [np.inf]
    if custom_labels is not None and len(custom_labels) != len(bin_edges) - 1:
        raise ValueError(
            f"Thresholds {thresholds} produce {len(bin_edges)-1} classes, but only {len(custom_labels)} custom labels were provided"
        )

    df = df.copy()
    df = df.sort_values(["symbol", "timeframe"])
    future_price_col = f"{price_col}_future_{steps}"
    return_col = f"future_return_{steps}"
    df[future_price_col] = df.groupby("symbol")[price_col].shift(-steps)
    df[return_col] = df[future_price_col] / df[price_col] - 1

    df["label"] = pd.cut(
        df[return_col],
        bins=bin_edges,
        labels=False if custom_labels is None else custom_labels,
        include_lowest=True,
    )
    return df
