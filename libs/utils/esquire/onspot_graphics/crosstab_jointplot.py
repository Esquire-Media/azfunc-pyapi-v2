import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from io import BytesIO


def crosstab_jointplot(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    x_order: list = None,
    y_order: list = None,
    aspect_ratio: float = 0.5,
    xtick_rotation: int = 90,
    ytick_rotation: int = 0,
    heatmap_palette: str = "Blues",
    xbar_color: str = "#2697de",
    ybar_color: str = "#2697de",
    export_path: str = None,
    return_bytes:bool = False
):
    """
    Returns a heatmap of the crosstabs between two dataframe columns, with totals bars.

    Params:
    df              :   The dataframe on which to generate crosstabs.
    x_col           :   The column to represent on the X axis
    y_col           :   The column to represent on the Y axis
    x_order         :   Optional sort order for the X axis. Alphabetical sorting will be used if no value is passed.
    y_order         :   Optional sort order for the Y axis. Alphabetical sorting will be used if no value is passed.
    aspect_ratio    :   Float value to scale the ratio between the text and the image size. Recommended between 0.25 and 2 for most cases.
    xtick_rotation  :   Rotation for X ticks.
    ytick_rotation  :   Rotation for X ticks.
    heatmap_palette :   Plotly color palette for the central heatmap grid.
    xbar_color      :   Color for the X axis totals bars.
    ybar_color      :   Color for the Y axis totals bars.

    """
    # get non-null crosstabs, normalized so 1 is the "average" cell value
    crosstab = pd.crosstab(df[x_col], df[y_col], normalize=True)

    # populate age brackets with no data
    for age_cat in y_order:
        if age_cat not in crosstab.columns:
            crosstab[age_cat] = 0
    # populate income brackets with no data
    for inc_cat in x_order:
        if inc_cat not in crosstab.index:
            row = {age_cat:0 for age_cat in y_order}
            crosstab.loc[inc_cat] = row

    # set a default sort order if none is defined
    if x_order == None:
        x_order = sorted(df[x_col].unique().tolist())
    if y_order == None:
        y_order = sorted(df[y_col].unique().tolist())

    # stacked format needed for the jointplot
    stack = crosstab.stack().reset_index().rename(columns={0: "weight"})
    # sort the stacked values using the passed range orders
    stack = sort_df_by_list(df=stack, col_name=y_col, sort_list=y_order)
    stack = sort_df_by_list(df=stack, col_name=x_col, sort_list=x_order)

    # JOINTPLOT AND HEATMAP COMBINED GRAPH
    # graphing params
    nx = crosstab.shape[0]
    ny = crosstab.shape[1]

    # skeleton for the totals bars
    g = sns.jointplot(data=stack, x=x_col, y=y_col, kind="hist", bins=(nx, ny))
    g.ax_marg_x.cla()
    g.ax_marg_y.cla()

    # # interior grid heatmap on top of skeleton
    heat = sns.heatmap(
        data=crosstab.loc[x_order].T.loc[y_order],
        ax=g.ax_joint,
        cbar=False,
        cmap=heatmap_palette,
        square=True,
    )

    # set values for totals bars
    g.ax_marg_x.bar(
        np.arange(0.5, nx),
        stack.groupby([x_col], sort=False)["weight"].sum().to_numpy(),
        color=xbar_color,
    )
    g.ax_marg_y.barh(
        np.arange(0.5, ny),
        stack.groupby([y_col], sort=False)["weight"].sum().to_numpy(),
        color=ybar_color,
    )

    # # # remove ticks between heatmao and histograms
    g.ax_marg_x.tick_params(axis="x", bottom=False, labelbottom=False)
    g.ax_marg_y.tick_params(axis="y", left=False, labelleft=False)
    # remove ticks showing the heights of the histograms
    g.ax_marg_x.tick_params(axis="y", left=False, labelleft=False)
    g.ax_marg_y.tick_params(axis="x", bottom=False, labelbottom=False)
    # less space needed when tick labels are removed
    g.fig.subplots_adjust(hspace=0.05, wspace=0.02)

    # tick parameters
    heat.set_xticklabels(x_order, rotation=xtick_rotation)
    heat.set_yticklabels(y_order, rotation=ytick_rotation)
    heat.set_xlabel(x_col, size=8)
    heat.set_ylabel(y_col, size=8)

    # set final size as a ratio of nx and ny
    g.fig.set_size_inches(nx * aspect_ratio, ny * aspect_ratio)

    # plt.tight_layout()
    # export if specified
    if return_bytes:
        buffer = BytesIO()
        plt.savefig(buffer, facecolor="white", bbox_inches="tight")
        plt.close()
        buffer.seek(0)
        return buffer
    if export_path != None:
        plt.savefig(fname=export_path, facecolor="white", bbox_inches="tight")
        plt.close()
    else:
        plt.show()

def sort_df_by_list(df: pd.DataFrame, col_name: str, sort_list: list) -> pd.DataFrame:
    """
    Sort a pandas dataframe on a column using a custom list to define the sort order.
    """
    # Create a dummy df with the required list and the col name to sort on
    dummy = pd.Series(sort_list, name=col_name).to_frame()

    # Use left merge on the dummy to return a sorted df
    sorted_df = pd.merge(dummy, df, on=col_name, how="left")
    return sorted_df
