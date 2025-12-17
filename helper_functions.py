

import pandas as pd
import numpy as np

def AddDateColumns(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """
  Expand a date column into multiple useful time-based fields.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataset containing a date column.
    date_col : str
        Name of the column with date-like values (string or datetime).

    Returns
    -------
    pd.DataFrame
        A copy of the DataFrame with added:
          - Year (YYYY)
          - Quarter (YYYY-Q)
          - Month (YYYY-MM)
    
    Notes
    -----
    - The function coerces invalid date values to NaT.
    - Useful for aggregation and time-series analysis.

    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    # Extract month, year, quarter
    month = df[date_col].dt.month
    year = df[date_col].dt.year
    quarter = df[date_col].dt.quarter

    df['Year'] = year.astype(str)
    df['Quarter'] = year.astype(str) + '-' + quarter.astype(str)
    df['Month'] = year.astype(str) + '-' + month.astype(str).str.zfill(2)

    return df


def sort_by_date(df_subset, frequency):
    """
     Sort a subset of the dataset by its temporal frequency.

    Parameters
    ----------
    df_subset : pd.DataFrame
        DataFrame containing a 'Date' column formatted as:
        - Year: "YYYY"
        - Quarter: "YYYY-Q"
        - Month: "YYYY-MM"
    frequency : str
        One of {"Year", "Quarter", "Month"}.

    Returns
    -------
    pd.DataFrame
        The input DataFrame sorted by Measure and the appropriate
        time components. Temporary parsing columns are removed.

    Notes
    -----
    - Quarter and Month strings are split into numeric components
      to ensure correct chronological ordering.
    """
    if frequency == "Year":
        df_subset["Year"] = df_subset["Date"].astype(int)
        df_subset = df_subset.sort_values(by=["Measure", "Year"])
        df_subset = df_subset.drop(columns=["Year"])

    elif frequency == "Quarter":
        df_subset[["Year", "QuarterNum"]] = df_subset["Date"].str.split("-", expand=True)
        df_subset["Year"] = df_subset["Year"].astype(int)
        df_subset["QuarterNum"] = df_subset["QuarterNum"].astype(int)
        df_subset = df_subset.sort_values(by=["Measure", "Year", "QuarterNum"])
        df_subset = df_subset.drop(columns=["Year", "QuarterNum"])

    else:  # assume Month
        df_subset[["Year", "MonthNum"]] = df_subset["Date"].str.split("-", expand=True)
        df_subset["Year"] = df_subset["Year"].astype(int)
        df_subset["MonthNum"] = df_subset["MonthNum"].astype(int)
        df_subset = df_subset.sort_values(by=["Measure", "Year", "MonthNum"])
        df_subset = df_subset.drop(columns=["Year", "MonthNum"])

    return df_subset


def aggregate_dataframe(df, date_col, group_by_cols, sum_cols, frequency):
    """
        Aggregate a dataset at a specified time frequency and compute measure rates.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataset. Must contain the date column and any grouping columns.
    date_col : str
        Column representing the date at the desired frequency
        ("Year", "Quarter", "Month").
    group_by_cols : list
        List of grouping columns (e.g., ['Measure', 'Definitions', 'Multiplier']).
    sum_cols : list
        Columns to sum, typically ['Numerator', 'Denominator', 'Expected Events'].
    frequency : str
        Label used to identify the aggregation level.

    Returns
    -------
    pd.DataFrame
        Aggregated dataset with:
        - Summed numerator, denominator, and expected events
        - Calculated rate based on numerator/denominator/multiplier
        - Standardized Date column
        - Sorted rows via `sort_by_date`
    """
   
    group_by_cols = list(group_by_cols)
    if date_col not in group_by_cols:
        group_by_cols.append(date_col)

    # Subset
    df_subset = df[group_by_cols + sum_cols]

    # Aggregate
    agg_df = (
        df_subset.groupby(group_by_cols)[sum_cols]
                 .sum()
                 .reset_index()
    )

    # Add calculated fields
    agg_df["frequency"] = frequency
    agg_df["Rate"] = (agg_df["Numerator"] / agg_df["Denominator"]) * agg_df["Multiplier"]
    

    # Rename date column
    agg_df = agg_df.rename(columns={date_col: "Date"})

    # Sort using helper
    agg_df = sort_by_date(agg_df, frequency)

    # ------------------------------------------------------------
    # ADD REVERSE ROW NUMBER: newest date = 1
    #this will be helpful when creating the dashboard
    # ------------------------------------------------------------
    agg_df["RowNumber"] = (
        agg_df.groupby("Measure")
              .cumcount(ascending=False) + 1
    )


    return agg_df





def rolling_spc_agg(df, window_size=5, sigma=3):
    """
    Apply rolling-window Statistical Process Control (SPC) calculations 
    separately for each 'Measure' category.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing a 'Rate' column and a 'Measure' column.
        Should already be sorted by time.
    window_size : int, optional
        Number of previous periods to include in the rolling calculations (default = 5).
    sigma : int, optional
        Number of standard deviations to define control limits (default = 3).

    Returns
    -------
    pd.DataFrame
        DataFrame with added SPC columns:
          - Rolling_CL : rolling center line (mean) per Measure
          - Rolling_Std : rolling standard deviation per Measure
          - Rolling_UCL / Rolling_LCL : control limits per Measure
          - Is_Anomaly : 1 if the Rate is outside the control limits, else 0

    Notes
    -----
    - will only compute if enough history exists window_size = min_periods .
    - LCL is clipped at zero because negative event rates are not meaningful.
    - All calculations are performed **separately for each Measure**, so values 
      from different Measures are never mixed.
    """
    def calc_rolling(group):
        # Rolling mean (center line)
        group['Rolling_CL'] = group['Rate'].rolling(
            window=window_size, min_periods=window_size, closed='left'
        ).mean()

        # Rolling standard deviation
        group['Rolling_Std'] = group['Rate'].rolling(
            window=window_size, min_periods=window_size, closed='left'
        ).std()

        # Control limits
        group['Rolling_UCL_1STD'] = group['Rolling_CL'] + 1 * group['Rolling_Std']
        group['Rolling_LCL_1STD'] = (group['Rolling_CL'] - 1 * group['Rolling_Std']).clip(lower=0)

        group['Rolling_UCL_2STD'] = group['Rolling_CL'] + 2 * group['Rolling_Std']
        group['Rolling_LCL_2STD'] = (group['Rolling_CL'] - 2 * group['Rolling_Std']).clip(lower=0)

        group['Rolling_UCL_3STD'] = group['Rolling_CL'] + 3 * group['Rolling_Std']
        group['Rolling_LCL_3STD'] = (group['Rolling_CL'] - 3 * group['Rolling_Std']).clip(lower=0)


        return group
    df_out = df.groupby('Measure', group_keys=False).apply(calc_rolling)

    # ------------------------------------------
    # Null out SPC columns for yearly rows
    # ------------------------------------------
    mask_year = df_out["frequency"] == "Year"
    spc_cols = [
        "Rolling_CL", "Rolling_Std",
        "Rolling_UCL_1STD", "Rolling_LCL_1STD",
        "Rolling_UCL_2STD", "Rolling_LCL_2STD",
        "Rolling_UCL_3STD", "Rolling_LCL_3STD"
    ]

    df_out.loc[mask_year, spc_cols] = np.nan

    # Apply rolling calculations separately for each Measure
    return df_out
