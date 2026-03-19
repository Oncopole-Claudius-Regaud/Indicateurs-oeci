
from __future__ import annotations

import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


DAG_DIR = Path(__file__).resolve().parent
if str(DAG_DIR) not in sys.path:
    sys.path.append(str(DAG_DIR))

from utils.postgres_to_csv import export_sql_to_csv  # noqa: E402
from utils.push_matches_to_postgres import execute_sql_file, load_matches_tsv  # noqa: E402


PROJECT_ROOT = Path(os.getenv("RECORD_LINKAGE_ROOT", "/home/administrateur/record_linkage_insee"))
AIRFLOW_DATA_DIR = Path(os.getenv("INSEE_AIRFLOW_DATA_DIR", "/home/administrateur/airflow/data"))
INSEE_TXT_FILENAME = "insee_deces_latest.txt"
INSEE_TXT_REPO_DIR = PROJECT_ROOT / "insee" / "download" / "data"
INSEE_TXT_FILTER_REGEX = r"^insee_deces_latest\.txt$"
INSEE_CSV_FILTER_REGEX = r"^insee_deces_latest\.csv$"
INSEE_NDJSON_GLOB = "insee_deces_latest*.NDJSON"
INSEE_HISTORICAL_START_YEAR = 2020
INSEE_HISTORICAL_TXT_FILTER_REGEX = r"^deces-(202[0-9]|20[3-9][0-9])(-m[0-9]{2})?\.txt$"
INSEE_HISTORICAL_CSV_FILTER_REGEX = r"^deces-(202[0-9]|20[3-9][0-9])(-m[0-9]{2})?\.csv$"
# Set to "historical" for one full bootstrap, then switch back to "monthly".
INSEE_RUN_MODE = "historical"

if INSEE_RUN_MODE not in {"monthly", "historical"}:
    raise ValueError("INSEE_RUN_MODE must be one of: monthly, historical")

POSTGRES_CONN_ID = os.getenv("OECI_POSTGRES_CONN_ID", "postgres_test")
PATIENT_SQL_PATH = DAG_DIR / "utils" / "sql" / "patients_trackcare_extract.sql"
PATIENT_CSV_PATH = PROJECT_ROOT / "rapprochement" / "probabilist" / "all_patients" / "input" / "patients_trackcare.csv"
MATCH_TSV_PATH = PROJECT_ROOT / "rapprochement" / "probabilist" / "all_patients" / "results" / "patients_trackcare_matches.tsv"
MERGE_MATCHES_SQL_PATH = DAG_DIR / "utils" / "sql" / "merge_insee_matches.sql"

UNLOCK_INDEX_COMMAND = (
    "curl -s -X PUT 'http://127.0.0.1:9200/insee/_settings' "
    "-H 'Content-Type: application/json' "
    "-d '{\"index.blocks.read_only_allow_delete\": null}' >/dev/null || true; "
)


def _fetch_insee_txt_resources() -> list[dict[str, Any]]:
    dataset_api = "https://www.data.gouv.fr/api/1/datasets/fichier-des-personnes-decedees/"
    request = Request(dataset_api, headers={"User-Agent": "airflow-record-linkage"})
    with urlopen(request, timeout=90) as response:
        payload = json.loads(response.read().decode("utf-8"))

    resources = payload.get("resources", [])
    filename_regex = re.compile(r"deces-(\d{4})(?:-m(\d{1,2}))?\.txt$", re.IGNORECASE)

    by_filename: dict[str, dict[str, Any]] = {}
    for resource in resources:
        url = (resource.get("url") or "").strip()
        if ".txt" not in url.lower():
            continue

        probe = " ".join(
            filter(
                None,
                [
                    resource.get("filename"),
                    resource.get("title"),
                    url,
                ],
            )
        )
        match = filename_regex.search(probe)
        if not match:
            continue

        year = int(match.group(1))
        if year < INSEE_HISTORICAL_START_YEAR:
            continue
        month = int(match.group(2)) if match.group(2) else 0
        rank = (year, month)
        filename = f"deces-{year:04d}{f'-m{month:02d}' if month else ''}.txt"
        by_filename[filename] = {
            "filename": filename,
            "url": url,
            "rank": rank,
        }

    resources_out = sorted(by_filename.values(), key=lambda item: item["rank"])
    if not resources_out:
        raise RuntimeError("No deces-*.txt resource found on data.gouv.fr")
    return resources_out


def _download_txt(url: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    request = Request(url, headers={"User-Agent": "airflow-record-linkage"})
    with urlopen(request, timeout=600) as source, open(output_path, "wb") as target:
        shutil.copyfileobj(source, target)


def _download_latest_insee_txt() -> None:
    resources = _fetch_insee_txt_resources()
    latest = resources[-1]
    AIRFLOW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = AIRFLOW_DATA_DIR / INSEE_TXT_FILENAME
    _download_txt(url=latest["url"], output_path=output_path)


def _download_all_insee_txt() -> None:
    resources = _fetch_insee_txt_resources()
    INSEE_TXT_REPO_DIR.mkdir(parents=True, exist_ok=True)

    for item in resources:
        output_path = INSEE_TXT_REPO_DIR / item["filename"]
        if output_path.exists() and output_path.stat().st_size > 0:
            continue
        _download_txt(url=item["url"], output_path=output_path)


def _export_patients_csv() -> None:
    export_sql_to_csv(
        postgres_conn_id=POSTGRES_CONN_ID,
        sql_path=str(PATIENT_SQL_PATH),
        output_csv=str(PATIENT_CSV_PATH),
    )


def _load_matches_to_postgres_raw() -> None:
    load_matches_tsv(
        postgres_conn_id=POSTGRES_CONN_ID,
        tsv_path=str(MATCH_TSV_PATH),
        schema="oeci",
        table="insee_deces_matches_raw",
    )


def _merge_matches_to_postgres() -> None:
    execute_sql_file(
        postgres_conn_id=POSTGRES_CONN_ID,
        sql_path=str(MERGE_MATCHES_SQL_PATH),
    )


with DAG(
    dag_id="etl_insee_deces",
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["insee", "oeci", "record-linkage", f"mode:{INSEE_RUN_MODE}"],
) as dag:
    if INSEE_RUN_MODE == "historical":
        download_insee = PythonOperator(
            task_id="download_historical_insee_txt",
            python_callable=_download_all_insee_txt,
        )

        clean_prepare_outputs = BashOperator(
            task_id="clean_prepare_outputs",
            bash_command=(
                "set -euo pipefail; "
                f"mkdir -p '{PROJECT_ROOT}/insee/prepare/csv' '{PROJECT_ROOT}/insee/prepare/elasticsearch/NDJSON'; "
                f"find '{PROJECT_ROOT}/insee/prepare/csv' -maxdepth 1 -type f -name '*.csv' -delete; "
                f"find '{PROJECT_ROOT}/insee/prepare/elasticsearch/NDJSON' -maxdepth 1 -type f -name '*.NDJSON' -delete"
            ),
        )

        txt_to_csv = BashOperator(
            task_id="insee_txt_to_csv_historical",
            bash_command=(
                "set -euo pipefail; "
                f"cd '{PROJECT_ROOT}/insee/prepare'; "
                f"INSEE_TXT_FILTER='{INSEE_HISTORICAL_TXT_FILTER_REGEX}' Rscript 0.to_csv.R"
            ),
        )

        csv_to_ndjson = BashOperator(
            task_id="insee_csv_to_ndjson_historical",
            bash_command=(
                "set -euo pipefail; "
                f"cd '{PROJECT_ROOT}/insee/prepare'; "
                f"INSEE_CSV_FILTER='{INSEE_HISTORICAL_CSV_FILTER_REGEX}' Rscript 1.elasticSearch.R"
            ),
        )

        recreate_insee_index = BashOperator(
            task_id="recreate_insee_index",
            bash_command=(
                "set -euo pipefail; "
                "curl -s -X DELETE 'http://127.0.0.1:9200/insee' -H 'Content-Type: application/json' >/dev/null || true; "
                f"cd '{PROJECT_ROOT}/insee/prepare/elasticsearch'; "
                "bash 1.createIndex.sh; "
            ),
        )

        load_ndjson = BashOperator(
            task_id="load_ndjson_to_elastic_historical",
            bash_command="set -euo pipefail; "
            + UNLOCK_INDEX_COMMAND
            + f"cd '{PROJECT_ROOT}/insee/prepare/elasticsearch'; "
            + "bash loadNDJSON.sh NDJSON; "
            + "curl -s 'http://127.0.0.1:9200/insee/_count?pretty'",
        )

        download_insee >> clean_prepare_outputs >> txt_to_csv >> csv_to_ndjson >> recreate_insee_index >> load_ndjson
    else:
        download_insee = PythonOperator(
            task_id="download_latest_insee_txt",
            python_callable=_download_latest_insee_txt,
        )

        sync_txt_to_repo = BashOperator(
            task_id="sync_txt_to_repo",
            bash_command=(
                "set -euo pipefail; "
                f"mkdir -p '{INSEE_TXT_REPO_DIR}'; "
                f"cp -f '{AIRFLOW_DATA_DIR / INSEE_TXT_FILENAME}' '{INSEE_TXT_REPO_DIR / INSEE_TXT_FILENAME}'"
            ),
        )

        clean_latest_prepare_outputs = BashOperator(
            task_id="clean_latest_prepare_outputs",
            bash_command=(
                "set -euo pipefail; "
                f"find '{PROJECT_ROOT}/insee/prepare/csv' -maxdepth 1 -type f -name 'insee_deces_latest.csv' -delete; "
                f"find '{PROJECT_ROOT}/insee/prepare/elasticsearch/NDJSON' -maxdepth 1 -type f -name 'insee_deces_latest*.NDJSON' -delete"
            ),
        )

        txt_to_csv = BashOperator(
            task_id="insee_txt_to_csv_monthly",
            bash_command=(
                "set -euo pipefail; "
                f"cd '{PROJECT_ROOT}/insee/prepare'; "
                f"INSEE_TXT_FILTER='{INSEE_TXT_FILTER_REGEX}' Rscript 0.to_csv.R"
            ),
        )

        csv_to_ndjson = BashOperator(
            task_id="insee_csv_to_ndjson_monthly",
            bash_command=(
                "set -euo pipefail; "
                f"cd '{PROJECT_ROOT}/insee/prepare'; "
                f"INSEE_CSV_FILTER='{INSEE_CSV_FILTER_REGEX}' Rscript 1.elasticSearch.R"
            ),
        )

        ensure_insee_index = BashOperator(
            task_id="ensure_insee_index",
            bash_command=(
                "set -euo pipefail; "
                "if curl -s -f 'http://127.0.0.1:9200/insee' >/dev/null; then "
                "echo 'Index insee already exists - incremental load mode'; "
                "else "
                f"cd '{PROJECT_ROOT}/insee/prepare/elasticsearch'; "
                "bash 1.createIndex.sh; "
                "fi; "
            ),
        )

        load_ndjson = BashOperator(
            task_id="load_ndjson_to_elastic_monthly",
            bash_command="set -euo pipefail; "
            + UNLOCK_INDEX_COMMAND
            + f"cd '{PROJECT_ROOT}/insee/prepare/elasticsearch'; "
            + f"bash loadNDJSON.sh NDJSON '{INSEE_NDJSON_GLOB}'; "
            + "curl -s 'http://127.0.0.1:9200/insee/_count?pretty'",
        )

        download_insee >> sync_txt_to_repo >> clean_latest_prepare_outputs >> txt_to_csv >> csv_to_ndjson >> ensure_insee_index >> load_ndjson

    export_patients_csv = PythonOperator(
        task_id="export_patients_trackcare_csv",
        python_callable=_export_patients_csv,
    )

    run_linkage_from_csv = BashOperator(
        task_id="run_record_linkage_from_csv",
        bash_command=(
            "set -euo pipefail; "
            f"cd '{PROJECT_ROOT}/rapprochement/probabilist/all_patients'; "
            f"Rscript predict_from_csv.R '{PATIENT_CSV_PATH}' '{MATCH_TSV_PATH}'"
        ),
    )

    load_matches_to_postgres_raw = PythonOperator(
        task_id="load_matches_to_postgres_raw",
        python_callable=_load_matches_to_postgres_raw,
    )

    merge_matches_to_postgres = PythonOperator(
        task_id="merge_matches_to_postgres",
        python_callable=_merge_matches_to_postgres,
    )

    (
        load_ndjson
        >> export_patients_csv
        >> run_linkage_from_csv
        >> load_matches_to_postgres_raw
        >> merge_matches_to_postgres
    )

