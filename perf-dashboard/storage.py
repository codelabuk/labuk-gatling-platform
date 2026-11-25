"""Run results in S3 / MinIO: summaries, run folders, report files."""
import json
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

from minio import Minio
from minio.deleteobjects import DeleteObject
from minio.error import S3Error

import config

_client = None
_cache = {}          # key -> (expires, value)
CACHE_SECONDS = 30


def _minio():
    global _client
    if _client is None and config.STORAGE == "folder":
        from folder_store import FolderClient
        _client = FolderClient(config.STORAGE_DIR)
    if _client is None:
        u = urlparse(config.S3_ENDPOINT if "://" in config.S3_ENDPOINT else "http://" + config.S3_ENDPOINT)
        _client = Minio(u.netloc, access_key=config.S3_ACCESS_KEY, secret_key=config.S3_SECRET_KEY,
                        secure=u.scheme == "https", region=config.S3_REGION)
    return _client


def split_url(url):
    """s3://bucket/key -> (bucket, key)"""
    rest = url[len("s3://"):] if url.startswith("s3://") else url
    bucket, _, key = rest.partition("/")
    return bucket, key.strip("/")


def get_json(bucket, key):
    try:
        resp = _minio().get_object(bucket, key)
        try:
            return json.loads(resp.read())
        finally:
            resp.close()
            resp.release_conn()
    except S3Error as e:
        if e.code in ("NoSuchKey", "NoSuchBucket"):
            return None
        raise


def list_json(bucket, prefix, suffix=".json"):
    out = []
    for obj in _minio().list_objects(bucket, prefix=prefix.rstrip("/") + "/", recursive=False):
        if not obj.is_dir and obj.object_name.endswith(suffix):
            data = get_json(bucket, obj.object_name)
            if data is not None:
                out.append(data)
    return out


def run_summary(run_dir):
    """summary.json of a finished run + worker-N.json of its workers (None where missing)."""
    bucket, key = split_url(run_dir)
    return {
        "run": get_json(bucket, f"{key}/report/summary.json"),
        "workers": sorted(list_json(bucket, f"{key}/logs"), key=lambda w: w.get("worker", 0)),
    }


def list_runs(limit=200):
    """Run folders under RESULTS_URL with their summary (cached briefly - this does one GET per run)."""
    bucket, prefix = config.results_bucket_and_prefix()
    cache_key = ("runs", bucket, prefix)
    hit = _cache.get(cache_key)
    if hit and hit[0] > time.time():
        return hit[1]
    folders = [o.object_name.rstrip("/") for o in _minio().list_objects(bucket, prefix=prefix + "/" if prefix else "", recursive=False) if o.is_dir]
    folders = folders[:limit]

    def one(folder):
        summary = get_json(bucket, f"{folder}/report/summary.json")
        return {
            "folder": folder.split("/")[-1],
            "url": f"s3://{bucket}/{folder}",
            "report": f"{folder}/report/merged/index.html" if summary else None,
            "summary": summary,
        }

    with ThreadPoolExecutor(max_workers=8) as pool:
        runs = list(pool.map(one, folders))
    runs.sort(key=lambda r: (r["summary"] or {}).get("start") or "", reverse=True)
    _cache[cache_key] = (time.time() + CACHE_SECONDS, runs)
    return runs


def report_object(key):
    """(stream, content length) of a report file; the caller closes the stream."""
    bucket, _ = config.results_bucket_and_prefix()
    resp = _minio().get_object(bucket, key)
    return resp, int(resp.headers.get("Content-Length", 0) or 0)


def delete_prefix(bucket, prefix):
    """Deletes every object under prefix (which must end with '/'); returns how many. Cannot be undone."""
    if not prefix.endswith("/") or prefix == "/":
        raise ValueError("refusing to delete without a folder prefix")
    names = [o.object_name for o in _minio().list_objects(bucket, prefix=prefix, recursive=True)]
    if names:
        errors = list(_minio().remove_objects(bucket, [DeleteObject(n) for n in names]))
        if errors:
            raise RuntimeError(f"could not delete {len(errors)} of {len(names)} object(s): {errors[0]}")
    _cache.clear()   # the History list must not show a deleted run
    return len(names)


def run_folder_url(folder):
    bucket, prefix = config.results_bucket_and_prefix()
    return f"s3://{bucket}/{prefix + '/' if prefix else ''}{folder}"


def delete_run(folder):
    """Deletes one run folder (logs, report, summary, test snapshot) under RESULTS_URL."""
    bucket, prefix = config.results_bucket_and_prefix()
    return delete_prefix(bucket, f"{prefix + '/' if prefix else ''}{folder}/")
