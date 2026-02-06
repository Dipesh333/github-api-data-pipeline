from datetime import datetime, timezone
from pathlib import Path
import logging

from extract.github_client import fetch_repo, fetch_commits
from load.json_writer import write_raw_json
from transform.silver_commits import build_silver_commits
from gold.dim_repo import build_dim_repo
from gold.dim_author import build_dim_author
from gold.dim_date import build_dim_date
from gold.fact_commits import build_fact_commits
from state.state_manager import update_last_commit_ts
from quality.checks import validate_fact_commits

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    try:
        
        owner = "apache"
        repo = "airflow"
        repo_name = f"{owner}/{repo}"

        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        logger.info("Pipeline started")

        # ---- EXTRACT ----
        repo_data = fetch_repo(owner, repo)
        write_raw_json("repos", [repo_data])

        commits, latest_commit_ts = fetch_commits(owner, repo)
        write_raw_json("commits", commits)

        # ---- TRANSFORM ----
        raw_commits = Path(f"data/raw/commits/run_date={run_date}/commits.json")
        silver_commits = Path("data/silver/commits/commits.csv")

        build_silver_commits(raw_commits, silver_commits, repo_name)

        # ---- GOLD ----
        dim_repo = Path("data/gold/dim_repo.csv")
        dim_author = Path("data/gold/dim_author.csv")
        dim_date = Path("data/gold/dim_date.csv")
        fact_commits = Path("data/gold/fact_commits.csv")

        build_dim_repo(silver_commits, dim_repo)
        build_dim_author(silver_commits, dim_author)
        build_dim_date(silver_commits, dim_date)
        build_fact_commits(silver_commits, dim_author, dim_repo, dim_date, fact_commits)

        # ---- DATA QUALITY ----
        validate_fact_commits(fact_commits)
        logger.info("Data quality checks passed")

        # ---- STATE UPDATE (CRITICAL) ----
        if latest_commit_ts:
            update_last_commit_ts(latest_commit_ts)
            logger.info("Pipeline state updated")

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.exception("Pipeline failed")
        raise

if __name__ == "__main__":
    main()
