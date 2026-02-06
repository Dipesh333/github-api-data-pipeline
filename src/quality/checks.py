import pandas as pd
from datetime import datetime, timezone

def validate_fact_commits(fact_path: str) -> None:
    df = pd.read_csv(fact_path)

    # Primary key
    if df["commit_id"].isnull().any():
        raise ValueError("Null commit_id found in fact_commits")

    if df["commit_id"].duplicated().any():
        raise ValueError("Duplicate commit_id found in fact_commits")

    # Foreign keys
    if df["author_id"].isnull().any():
        raise ValueError("Null author_id found in fact_commits")

    if df["repo_id"].isnull().any():
        raise ValueError("Null repo_id found in fact_commits")

    if df["date_id"].isnull().any():
        raise ValueError("Null date_id found in fact_commits")

    # Volume
    if df.empty:
        raise ValueError("fact_commits is empty")
