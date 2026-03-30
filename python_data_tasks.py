import pandas as pd
import numpy as np


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows from a DataFrame.
    """
    return df.drop_duplicates()


def check_missing_values(df: pd.DataFrame) -> pd.Series:
    """
    Return missing value counts by column.
    """
    return df.isnull().sum()


def aggregate_sales_by_customer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate total amount by customer_id.
    Expected columns: customer_id, amount
    """
    return (
        df.groupby("customer_id", as_index=False)["amount"]
        .sum()
        .rename(columns={"amount": "total_sales"})
    )


def latest_record_per_customer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep latest record per customer based on order_date.
    Expected columns: customer_id, order_date
    """
    df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"])
    df = df.sort_values(["customer_id", "order_date"], ascending=[True, False])
    return df.drop_duplicates(subset=["customer_id"], keep="first")


def validate_row_counts(source_df: pd.DataFrame, target_df: pd.DataFrame) -> dict:
    """
    Compare row counts between source and target.
    """
    return {
        "source_count": len(source_df),
        "target_count": len(target_df),
        "match": len(source_df) == len(target_df),
    }


def validate_sum(source_df: pd.DataFrame, target_df: pd.DataFrame, column: str) -> dict:
    """
    Compare aggregate sums between source and target.
    """
    source_sum = source_df[column].sum()
    target_sum = target_df[column].sum()
    return {
        "column": column,
        "source_sum": source_sum,
        "target_sum": target_sum,
        "match": source_sum == target_sum,
    }


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize DataFrame column names.
    """
    df = df.copy()
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    return df


if __name__ == "__main__":
    data = {
        "customer_id": [101, 101, 102, 103, 103],
        "order_date": [
            "2025-01-01",
            "2025-02-01",
            "2025-01-15",
            "2025-01-05",
            "2025-03-01",
        ],
        "amount": [100, 150, 200, 120, 300],
    }

    df = pd.DataFrame(data)

    print("Original Data:")
    print(df)

    print("\nLatest Record Per Customer:")
    print(latest_record_per_customer(df))

    print("\nAggregate Sales By Customer:")
    print(aggregate_sales_by_customer(df))

    print("\nMissing Values:")
    print(check_missing_values(df))
