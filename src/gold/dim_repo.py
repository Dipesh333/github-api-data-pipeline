import pandas as pd
from pathlib import Path


def build_dim_repo(silver_commits_path: Path, dim_repo_path: Path) -> None:
    #Read Silver
    df = pd.read_csv(silver_commits_path)

    #Get unique repos
    repos = df[["repo_name"]].drop_duplicates().reset_index(drop=True)

    #Split owner/repo
    repos[["owner", "repo"]] = repos["repo_name"].str.split("/", n=1, expand=True)

    #Assign surrogate key
    repos.insert(0, "repo_id", range(1, len(repos) + 1))

    #Write Gold dimension
    dim_repo_path.parent.mkdir(parents=True, exist_ok=True)
    repos.to_csv(dim_repo_path, index=False)
