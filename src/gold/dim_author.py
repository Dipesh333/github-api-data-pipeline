import pandas as pd
from pathlib import Path


def build_dim_author(silver_commits_path: Path, dim_author_path: Path) -> None:
    #Read Silver commits
    df = pd.read_csv(silver_commits_path)

    #Select author attributes only
    authors = df[["author_name", "author_email"]]

    #Remove duplicate authors
    authors = authors.drop_duplicates().reset_index(drop=True)

    #Assign surrogate key
    authors.insert(0, "author_id", range(1, len(authors) + 1))

    #Ensure output directory exists
    dim_author_path.parent.mkdir(parents=True, exist_ok=True)

    #Write dimension table
    authors.to_csv(dim_author_path, index=False)
