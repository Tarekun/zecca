import pandas as pd
import matplotlib.pyplot as plt
from db.queries import select_ticker


def line_plot_df(
    df: pd.DataFrame,
    label: str,
    values: list[str],
    show: bool = False,
    plotname: str = "plot.png",
):
    """Creates a multi-line plot and saves it as a .png file under the plots directory.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame containing the data to plot. Must contain the columns specified
        in `label` and `values` parameters
    label : str
        The name of the column to use for the x-axis
    values : list[str]
        A list of column names to plot as separate lines on the y-axis. Each column
        will be plotted against the `label` column and appear as a separate line
        in the legend
    show : bool, optional
        If True, displays the plot using plt.show() in addition to saving it.
        Defaults to False.
    plotname : str, optional
        The filename for the saved plot. Should include the .png extension.
        The plot will be saved in the 'plots/' directory. Default is "plot.png".

    Raises
    ------
    ValueError
        If `label` is not a column in the DataFrame.
        If any column in `values` is not a column in the DataFrame."""

    if label not in df.columns:
        raise ValueError(f"'{label}' is not a column in the DataFrame.")
    for col in values:
        if col not in df.columns:
            raise ValueError(f"'{col}' is not a column in the DataFrame.")

    plt.figure(figsize=(10, 6))
    for col in values:
        plt.plot(df[label], df[col], label=col)
    plt.xlabel(label)
    plt.ylabel("Value")
    plt.legend()
    plt.tight_layout()

    plt.savefig(f"plots/{plotname}")
    if show:
        plt.show()

    # close the figure to avoid memory leaks
    plt.close()
