import pandas as pd
from pathlib import Path

def build_dim_date(silver_commits_path: Path, dim_date_path: Path) -> None:
    #Read Silver commits
    df = pd.read_csv(silver_commits_path)

    #Convert commit_date to datetime
    df["commit_date"] = pd.to_datetime(df["commit_date"])

    #Extract unique calendar dates
    dates = (
        df[["commit_date"]]
        .drop_duplicates()
        .rename(columns={"commit_date": "full_date"})
        .reset_index(drop=True)
    )

    #Derive date attributes
    dates["year"] = dates["full_date"].dt.year
    dates["month"] = dates["full_date"].dt.month
    dates["day"] = dates["full_date"].dt.day
    dates["day_of_week"] = dates["full_date"].dt.day_name()

    #Create surrogate key (YYYYMMDD)
    dates["date_id"] = dates["full_date"].dt.strftime("%Y%m%d").astype(int)

    #Reorder columns
    dates = dates[
        ["date_id", "full_date", "year", "month", "day", "day_of_week"]
    ]

    #Write dimension table
    dim_date_path.parent.mkdir(parents=True, exist_ok=True)
    dates.to_csv(dim_date_path, index=False)
