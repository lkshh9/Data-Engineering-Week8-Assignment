# Data-Engineering-Week8-Assignment

# NYC Taxi Dataset Analysis

## Introduction

This repository contains the solution to the NYC Taxi Dataset analysis assignment using PySpark in Databricks. The analysis includes various queries to derive insights from the dataset.

## Data Loading

The dataset is loaded into Databricks File System (DBFS) and then read into a PySpark DataFrame.

## Queries

1. **Add a "Revenue" Column:**
   - A new column named "Revenue" is added, which is the sum of several fare-related columns.

2. **Count of Total Passengers by Area:**
   - Total passengers are counted for each pickup location area.

3. **Real-time Average Fare/Earnings by Vendor:**
   - The average fare and total earnings are calculated for each vendor.

4. **Count of Payments by Payment Mode:**
   - The count of payments made by each payment mode is calculated.

5. **Top 2 Vendors on a Specific Date by Passengers and Distance:**
   - The top two vendors on a particular date are identified based on the number of passengers and total distance traveled.

6. **Most Passengers Between Two Locations:**
   - The route with the most passengers between two locations is identified.

7. **Top Pickup Locations in the Last 5/10 Seconds:**
   - The top pickup locations with the most passengers in the last 5/10 seconds are identified.

## Flattening JSON Fields

If the dataset contains JSON fields, they are flattened using the `json_tuple` function in PySpark.

## Writing as External Parquet Table

The final DataFrame is written as an external Parquet table to a specified location in the data lake.

## Instructions

1. Clone the repository.
2. Upload the dataset to Databricks File System (DBFS).
3. Execute the provided PySpark code in a Databricks notebook.
4. Verify the results of each query.

## Dependencies

- PySpark
- Databricks


