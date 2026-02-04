import pandas as pd
from pathlib import Path


def build_silver_commits(raw_commits_path: Path, silver_path: Path,repo_name: str) -> None:
    # 1) Read raw JSON
    df = pd.read_json(raw_commits_path)

    # 2) Flatten commit JSON
    commit_norm = pd.json_normalize(df["commit"])

    # 3) Build Silver table
    silver = pd.DataFrame({
        "commit_sha": df["sha"],
        "commit_date": commit_norm["author.date"],
        "author_name": commit_norm["author.name"],
        "author_email": commit_norm["author.email"],
        "committer_name": commit_norm["committer.name"],
        "commit_message": commit_norm["message"],
        "repo_name": repo_name, 
        "_extracted_at": df["_extracted_at"],
    })

    # 4) Write Silver output
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    silver.to_csv(silver_path, index=False)
