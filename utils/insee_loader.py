import logging
import os
import re
import time

import requests


HTTP_HEADERS = {"User-Agent": "oeci-insee-loader/1.0"}
MONTHLY_RE = re.compile(r"^deces-(\d{4})-m(\d{2})\.txt$", re.I)
ANNUAL_RE = re.compile(r"^deces-(\d{4})\.txt$", re.I)

DATASET_API_URL = "https://www.data.gouv.fr/api/1/datasets/fichier-des-personnes-decedees/"
LOCAL_DOWNLOAD_PATH = "/home/administrateur/record_linkage_insee/insee/download/data/insee_deces_latest.txt"


def _get_dataset_json(max_retries=3, backoff=2.0):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(DATASET_API_URL, headers=HTTP_HEADERS, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_err = exc
            logging.warning("[INSEE] attempt %s/%s failed: %s", attempt, max_retries, exc)
            time.sleep(backoff * attempt)
    raise Exception(f"[INSEE] failed to fetch INSEE metadata: {last_err}")


def _iter_resources():
    data = _get_dataset_json()
    for resource in data.get("resources", []):
        name = (
            resource.get("title")
            or resource.get("name")
            or resource.get("file_name")
            or ""
        ).strip()
        url = (
            resource.get("url")
            or (resource.get("latest") or {}).get("url")
            or ""
        ).strip()
        yield name, url


def _latest_monthly_resource():
    candidates = []
    for name, url in _iter_resources():
        match = MONTHLY_RE.match(name)
        if match and url:
            year = int(match.group(1))
            month = int(match.group(2))
            candidates.append(((year, month), name, url))
    if not candidates:
        raise Exception("[INSEE] no monthly file matching deces-YYYY-mMM.txt found")
    candidates.sort(key=lambda item: (item[0][0], item[0][1]), reverse=True)
    return candidates[0]


def _latest_annual_resource():
    candidates = []
    for name, url in _iter_resources():
        match = ANNUAL_RE.match(name)
        if match and url:
            year = int(match.group(1))
            candidates.append((year, name, url))
    if not candidates:
        raise Exception("[INSEE] no annual file matching deces-YYYY.txt found")
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0]


def download_insee_file(mode="monthly", **context):
    logging.info("[INSEE] download mode: %s", mode)

    if mode == "historical":
        year, name, url = _latest_annual_resource()
        filename = name or f"deces-{year}.txt"
    else:
        (year, month), name, url = _latest_monthly_resource()
        filename = name or f"deces-{year}-m{month:02d}.txt"

    if not url:
        raise Exception("[INSEE] resource selected but URL is empty")

    logging.info("[INSEE] downloading %s from %s", filename, url)
    response = requests.get(url, headers=HTTP_HEADERS, timeout=60)
    if response.status_code != 200 or not response.content:
        raise Exception(f"[INSEE] download failed with HTTP {response.status_code}")

    local_dir = os.path.dirname(LOCAL_DOWNLOAD_PATH)
    if local_dir:
        os.makedirs(local_dir, exist_ok=True)

    with open(LOCAL_DOWNLOAD_PATH, "wb") as file_obj:
        file_obj.write(response.content)

    logging.info("[INSEE] file saved at: %s (source: %s)", LOCAL_DOWNLOAD_PATH, filename)

    try:
        task_instance = context.get("ti")
        if task_instance:
            task_instance.xcom_push(key="insee_filename", value=filename)
    except Exception:
        pass
