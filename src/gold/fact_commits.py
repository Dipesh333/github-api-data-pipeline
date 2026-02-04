import pandas as pd
from pathlib import Path


def build_fact_commits(
    silver_commits_path: Path,
    dim_author_path: Path,
    dim_repo_path: Path,
    dim_date_path: Path,
    fact_commits_path: Path,
) -> None:
    
    #Read inputs
    silver = pd.read_csv(silver_commits_path)
    dim_author = pd.read_csv(dim_author_path)
    dim_repo = pd.read_csv(dim_repo_path)
    dim_date = pd.read_csv(dim_date_path)

    #Prepare join keys
    silver["commit_date"] = pd.to_datetime(silver["commit_date"])
    dim_date["full_date"] = pd.to_datetime(dim_date["full_date"])

    #Join author
    fact = silver.merge(
        dim_author,
        on=["author_name", "author_email"],
        how="left",
        validate="many_to_one",
    )

    #Join repo
    fact = fact.merge(
        dim_repo[["repo_id", "repo_name"]],
        on="repo_name",
        how="left",
        validate="many_to_one",
    )

    #Join date
    fact = fact.merge(
        dim_date[["date_id", "full_date"]],
        left_on="commit_date",
        right_on="full_date",
        how="left",
        validate="many_to_one",
    )

    #Select fact columns
    fact_commits = pd.DataFrame({
        "commit_id": fact["commit_sha"],
        "author_id": fact["author_id"],
        "repo_id": fact["repo_id"],
        "date_id": fact["date_id"],
        "commit_count": 1,
        "commit_message": fact["commit_message"],
        "_extracted_at": fact["_extracted_at"],
    })

    #Write output
    fact_commits_path.parent.mkdir(parents=True, exist_ok=True)
    fact_commits.to_csv(fact_commits_path, index=False)
