import polars as pl
import numpy as np
from matplotlib import pyplot as plt


def plot_label_distribution(df: pl.DataFrame, label_col: str):
    counts = df[label_col].value_counts().sort(label_col)
    labels = df[label_col].unique()
    plt.figure(figsize=(10, 6))
    plt.pie(x=counts["count"], labels=labels)
    plt.title("label distribution")
    plt.legend(
        labels,
        title="categories",
        bbox_to_anchor=(1, 0.5),
    )
    plt.show()
