from extract.github_client import fetch_repo, fetch_commits
from load.csv_writer import write_raw_csv


def main():
    owner = "apache"
    repo = "airflow"

    repo_data = fetch_repo(owner, repo)
    write_raw_csv("repos", [repo_data])

    commits = fetch_commits(owner, repo)
    write_raw_csv("commits", commits)

    print("Pipeline completed successfully")


if __name__ == "__main__":
    main()

