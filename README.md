📘 README — Synthetic Hospital Quality Data Pipeline
🏥 Overview

This project generates and processes a synthetic dataset representing a large hospital’s quality and safety performance. The dataset simulates realistic operational patterns for four major clinical measures:

HAPI (Hospital-Acquired Pressure Injuries)

Falls with Injury

CAUTI (Catheter-Associated Urinary Tract Infection)

CLABSI (Central Line–Associated Bloodstream Infection)

The purpose of this synthetic dataset is to support data exploration, quality-improvement analytics, SPC monitoring, and dashboard development, all without exposing any real patient or hospital information.

🔒 Important Note:

The dataset and this README were generated with assistance from ChatGPT.
No real patients, hospitals, or protected information are used.

Python scripts and the final dashboard are built entirely by the developer.

This project may be safely shared publicly and used for:

Training

Education

Analytics demonstrations

Portfolio work

🎯 Project Goals

Generate realistic large-hospital data from 2020–2025

Calculate denominators, expected events, and measure-specific rates

Aggregate results by Month, Quarter, and Year

Apply Statistical Process Control (SPC) logic using rolling windows

Produce a complete analytics-ready dataset

Support the creation of an interactive dashboard for monitoring trends and performance

🔧 Key Components
### 1. Synthetic Data Generator

Creates monthly records (2020–2025) with:

Four major quality measures

Total discharges

Patient days and device days

Expected event counts

Observed event counts (numerator)

Automatically assigned denominators

Values are generated to reflect patterns typical of a large hospital.

### 2. Helper Functions Module

The helper module (helper_functions.py) provides reusable analytic tools:

Time-based column creation (Year, Quarter, Month)

Flexible aggregation engine for multiple time granularities

Rate computation using multipliers (per 100 / per 1000)

SPC computation:

Rolling mean (CL)

Rolling standard deviation

UCL / LCL

Anomaly detection

These functions form the core analytics layer used throughout the pipeline.

### 3. Data Cleaning & Processing Pipeline

The script large_hospital_dataset_cleaning.py performs:

Type fixes and numeric rounding

Adding multipliers and measure definitions

Generating date hierarchy columns

Aggregating by Year, Quarter, Month

Calculating SPC values

Combining all outputs into a single dashboard-ready dataset

The result is a clean, structured dataset suitable for visualization and analytics.

📊 Final Dataset

The final exported CSV includes:

Measure name

Denominator type (per 100 or per 1000)

Numerator, Denominator, Expected Events

Calculated Rates

Frequency (Month, Quarter, Year)

SPC metrics: Center Line, UCL, LCL

Anomaly indicators

A sortable Date field

This enables comprehensive dashboarding, trending, and SPC signal interpretation.

📈 Dashboard

The processed dataset is designed to feed an interactive dashboard that visualizes:

Performance trends at multiple time levels

Observed vs expected comparisons

Control chart calculations

Anomaly detection

Measure-by-measure performance patterns

The dashboard is created by the developer using the synthetic dataset and transformation scripts.



🚀 Next Steps

Publish the dashboard online

Add automated tests for helper functions

Expand dataset to include more measures

Add example notebooks or visualizations

Package functions into a small analytics library