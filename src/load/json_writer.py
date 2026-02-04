import json
from datetime import datetime, timezone
from pathlib import Path


def write_raw_json(entity: str, rows: list[dict]) -> None:
    if not rows:
        return

    extracted_at = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["_extracted_at"] = extracted_at

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = Path(f"data/raw/{entity}/run_date={run_date}")
    path.mkdir(parents=True, exist_ok=True)

    file_path = path / f"{entity}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)
