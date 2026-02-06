import os
import requests
import logging
from datetime import datetime, timedelta, timezone
from state.state_manager import get_last_commit_ts

BASE_URL = "https://api.github.com"

logger= logging.getLogger(__name__)

def _get_headers():
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN not set")
    return {"Authorization": f"token {token}"}

def fetch_repo(owner: str, repo: str) -> dict:
    try:
        url = f"{BASE_URL}/repos/{owner}/{repo}"
        response = requests.get(url, headers=_get_headers())
        response.raise_for_status()
        return response.json()
    
    except Exception as e:
        logger.exception("Failed to fetch repository")
        raise

def fetch_commits(owner: str, repo: str, per_page: int = 100,lookback_days: int = 7) -> list[dict]:
    """Fetch all commits with pagination."""
    try:
        commits = []
        page = 1

        since_ts = get_last_commit_ts()  # None on first run

        while True:
            url = f"{BASE_URL}/repos/{owner}/{repo}/commits"
            params = {"per_page": per_page, "page": page}
            if since_ts:
                params["since"] = since_ts

            response = requests.get(url, headers=_get_headers(), params=params)
            response.raise_for_status()

            batch = response.json()
            if not batch:
                break

            commits.extend(batch)
            page += 1

        # compute new high-watermark
        latest_ts = None
        if commits:
            latest_ts = max(
                c["commit"]["committer"]["date"] for c in commits
            )

        return commits, latest_ts
    
    except Exception as e:
        logger.exception("Failed to fetch commits")
        raise

