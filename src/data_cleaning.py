# Data Cleaning and Transformation Module - Core ETL logic for the pipeline

import pandas as pd
import local_logging as ll

logger = ll.get_logger()


def combine_student_data(dataframe_1, dataframe_2):
    # Joins two student datasets on student_id using inner join
    joined_data = pd.merge(dataframe_1, dataframe_2, on="student_id", how="inner")
    return joined_data


def _remove_nulls(data):
    # Removes rows with any null/missing values and logs count
    null_counts = data.isnull().sum().sum()  # Count total null values across all columns

    logger.info(f"Removing {null_counts} rows due to null values")
    return data.dropna()  # Drop any row containing at least one null value


def _fix_departments(data):
    # Standardizes home_department by overwriting with major (source of truth)
    # Identify rows where home_department and major don't match
    mismatches = data[data["home_department"] != data["major"]]
    logger.info(
        f"Overwriting home_department for {len(mismatches)} rows based on major"
    )

    # Overwrite home_department with the value from major for ALL rows
    data.loc[:, "home_department"] = data["major"]
    return data


def process_student_data(data):
    # Main data processing pipeline - removes nulls and fixes department mismatches
    processed_nulls = _remove_nulls(data)  # Step 1: Clean nulls
    processed_majors = _fix_departments(processed_nulls)  # Step 2: Fix departments

    return processed_majors
