"""
Analytics pipeline for synthetic large-hospital dataset.

Steps:
1. Load and clean raw data
2. Assign measure definitions and rate multipliers
3. Expand date components (Year, Quarter, Month)
4. Aggregate data at multiple temporal granularities
5. Compute SPC rolling statistics
6. Export final structured dataset
"""

# ------------------------------------------------------------
# Import required libraries
# ------------------------------------------------------------
import pandas as pd
import numpy as np
from helper_functions import  AddDateColumns, aggregate_dataframe, rolling_spc_agg

# ------------------------------------------------------------
#  Read the dataset
# ------------------------------------------------------------
df = pd.read_csv('large_hospital_dataset.csv')

# ------------------------------------------------------------
#  Understand the data 
# ------------------------------------------------------------

#print(df.shape)
#print(df.dtypes)
#print(df.isnull().sum())
#print(df.describe())
#print(df.head())
#print(df.columns)

# ------------------------------------------------------------
#  The expected events column is a float and I want to round them then convert the values to integer 
# ------------------------------------------------------------
df['Expected Events'] = df['Expected Events'].round().astype('Int64')

# ------------------------------------------------------------
# Multiplier converts raw rate into standard "per X" unit for each measure
# ------------------------------------------------------------
# Map each measure to its multiplier value
df["Multiplier"] = df["Measure"].map({
    "HAPI": 100,
    "Falls with Injury": 1000,
    "CAUTI": 1000,
    "CLABSI": 1000
})

# ------------------------------------------------------------
# Define conditions for Definitions
# ------------------------------------------------------------
conditions = [
    df["Measure"] == "HAPI",
    df["Measure"] == "Falls with Injury",
    df["Measure"] == "CAUTI",
    df["Measure"] == "CLABSI"
]

# Descriptive labels for each denominator type
Definitions = [
    "Per 100 surveyed patients",  # HAPI
    "Per 1000 patient days",      # Falls with Injury
    "Per 1000 device days",       # CAUTI
    "Per 1000 device days"        # CLABSI
]

# ------------------------------------------------------------
# Assign human-readable rate definitions based on measure type
# ------------------------------------------------------------
df["Definitions"] = np.select(conditions, Definitions, default="Unknown")

#quick Check 
#print(df.head())

# ------------------------------------------------------------
# Step 6: Add date-related columns using helper function
# ------------------------------------------------------------
# This will expand "Discharge Month" into useful date parts (year, month, quarter, etc.)
df_w_date_columns = AddDateColumns(df, "Discharge Month")

# quick Check : Preview the transformed DataFrame
#print(df_w_date_columns.head())


# ------------------------------------------------------------
# Aggregate data to Year, Quarter, and Month levels for rate calculations
# ------------------------------------------------------------
group_by_cols = ['Measure', 'Definitions', 'Multiplier']
sum_cols = ['Numerator', 'Denominator', 'Expected Events']


# Aggregate by Year, Quarter, and Month
agg_year = aggregate_dataframe(df_w_date_columns, "Year", group_by_cols, sum_cols,"Year")
agg_quarter = aggregate_dataframe(df_w_date_columns, "Quarter", group_by_cols, sum_cols,"Quarter")
agg_month = aggregate_dataframe(df_w_date_columns, "Month",group_by_cols, sum_cols,"Month")

# ------------------------------------------------------------
# Apply rolling-window SPC calculations:
# - Rolling_CL = moving average
# - Rolling_UCL / LCL = (1,2 and 3)-sigma control limits
# Window sizes differ by frequency (24 mo, 8 qtr, 2 yr) I will not use the year

# ------------------------------------------------------------

agg_month_spc = rolling_spc_agg(agg_month, window_size=24)
agg_quarter_spc = rolling_spc_agg(agg_quarter, window_size=8)
agg_year_spc = rolling_spc_agg(agg_year, window_size=2)

# ------------------------------------------------------------
# Adjust the quarter name
# ------------------------------------------------------------
#print(agg_quarter_spc[['Date','frequency']].head())

agg_quarter_spc.loc[agg_quarter_spc['frequency'] == 'Quarter', 'Date'] = (
    agg_quarter_spc.loc[agg_quarter_spc['frequency'] == 'Quarter', 'Date']
      .str.replace(r"-(\d+)", r"-Q\1", regex=True)
)

#print(agg_quarter_spc[['Date','frequency']].head())
# ------------------------------------------------------------
# Combine all time granularities (Month, Quarter, Year) into one dataset
# ------------------------------------------------------------


Finalset = pd.concat([agg_month_spc, agg_quarter_spc, agg_year_spc], ignore_index=True)



print(Finalset.shape)
print(Finalset.columns)



Finalset.to_csv("NDNQI_synthetic_data .csv", index=False)