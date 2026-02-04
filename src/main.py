import logging
from datetime import datetime, timezone
from pathlib import Path
from transform.silver_commits import build_silver_commits
from extract.github_client import fetch_repo, fetch_commits
from load.json_writer import write_raw_json
from gold.dim_repo import build_dim_repo
from gold.dim_author import build_dim_author
from gold.dim_date import build_dim_date
from gold.fact_commits import build_fact_commits

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
logger=logging.getLogger(__name__)

def main():
    try:
        owner = "apache"
        repo = "airflow"

        # Compute run_date ONCE
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        logger.info("Pipeline Started")
        
        logger.info("Fetching repo")
        repo_data = fetch_repo(owner, repo)
        write_raw_json("repos", [repo_data])
        
        logger.info("Fetching commits")
        commits = fetch_commits(owner, repo)
        write_raw_json("commits", commits)

        raw_commits = Path(f"data/raw/commits/run_date={run_date}/commits.json")
        logger.info("Raw commits completed.")
        silver_commits = Path("data/silver/commits/commits.csv")
        
        repo_name = f"{owner}/{repo}"

        build_silver_commits(raw_commits,silver_commits,repo_name)
        logger.info("Silver commits completed")

        logger.info("Building dim_repo table")
        dim_repo = Path("data/gold/dim_repo.csv")
        build_dim_repo(silver_commits, dim_repo)
    
        logger.info("Building dim_author table")
        dim_author = Path("data/gold/dim_author.csv")
        build_dim_author(silver_commits, dim_author)
        
        logger.info("Building dim_date table")
        dim_date = Path("data/gold/dim_date.csv")
        build_dim_date(silver_commits, dim_date)

        logger.info("Building fact_commits table")
        fact_commits = Path("data/gold/fact_commits.csv")
        build_fact_commits(silver_commits,dim_author,dim_repo,dim_date,fact_commits)

        logger.info("Pipeline completed successfully")
    
    except Exception as e:
        logger.exception("Pipeline failed")
        raise

if __name__ == "__main__":
    main()
