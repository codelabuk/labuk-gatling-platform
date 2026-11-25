"""A folder that answers the MinIO client calls the dashboard makes - for running without S3 / MinIO.

s3://bucket/key maps to <root>/bucket/key, so the layout is the same as in MinIO (runs/<run>/logs/..., report/...,
test-plans/<suite>/...) and storage.py, plan_store.py and the report route work unchanged. Keys are checked so they
cannot leave the root.
"""
import io
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from minio.error import S3Error


def _missing(key):
    return S3Error("NoSuchKey", "The specified key does not exist.", key, "local", "local", None)


class _Object:
    """What get_object returns: read(), stream(n), headers, close(), release_conn()."""

    def __init__(self, path: Path):
        self._f = open(path, "rb")
        self.headers = {"Content-Length": str(path.stat().st_size)}

    def read(self):
        return self._f.read()

    def stream(self, amt=64 * 1024):
        while True:
            chunk = self._f.read(amt)
            if not chunk:
                break
            yield chunk

    def close(self):
        self._f.close()

    def release_conn(self):
        pass


class FolderClient:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, bucket, key=""):
        """The file for s3://bucket/key - refusing anything that would leave its bucket (or the root)."""
        if not bucket or "/" in bucket or "\\" in bucket or bucket in (".", ".."):
            raise ValueError(f"invalid bucket '{bucket}'")
        top = (self.root / bucket).resolve()
        p = (top / key).resolve()
        if p != top and top not in p.parents:
            raise ValueError(f"'{bucket}/{key}' is outside its bucket")
        return p

    # ------------------------------------------------------------------ reads

    def get_object(self, bucket, key):
        p = self.path(bucket, key)
        if not p.is_file():
            raise _missing(key)
        return _Object(p)

    def stat_object(self, bucket, key):
        p = self.path(bucket, key)
        if not p.is_file():
            raise _missing(key)
        return SimpleNamespace(size=p.stat().st_size)

    def list_objects(self, bucket, prefix="", recursive=False):
        """Like S3: keys starting with prefix; without recursive, one level, with folders as 'dir/' entries."""
        base = self.path(bucket)
        if not base.is_dir():
            return
        folder, _, start = prefix.rpartition("/")
        top = self.path(bucket, folder) if folder else base
        if not top.is_dir():
            return
        if recursive:
            for dirpath, _, files in os.walk(top):
                for f in sorted(files):
                    full = Path(dirpath) / f
                    key = full.relative_to(base).as_posix()
                    if key.startswith(prefix):
                        yield self._entry(key, full)
        else:
            for child in sorted(top.iterdir(), key=lambda c: c.name):
                key = child.relative_to(base).as_posix()
                if not key.startswith(prefix):
                    continue
                if child.is_dir():
                    yield SimpleNamespace(object_name=key + "/", is_dir=True, size=0, last_modified=None)
                elif child.is_file():
                    yield self._entry(key, child)

    @staticmethod
    def _entry(key, path):
        st = path.stat()
        return SimpleNamespace(object_name=key, is_dir=False, size=st.st_size,
                               last_modified=datetime.fromtimestamp(st.st_mtime, tz=timezone.utc))

    # ------------------------------------------------------------------ writes

    def put_object(self, bucket, key, data, length, content_type=None):
        p = self.path(bucket, key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp-upload")
        with open(tmp, "wb") as f:
            shutil.copyfileobj(io.BytesIO(data.read(length)) if length >= 0 else data, f)
        os.replace(tmp, p)  # readers never see a half-written file

    def remove_object(self, bucket, key):
        p = self.path(bucket, key)
        if p.is_file():
            p.unlink()
            self._prune(p.parent, self.path(bucket))

    def remove_objects(self, bucket, delete_list):
        top = self.path(bucket)
        for d in delete_list:
            p = self.path(bucket, d._name)
            if p.is_file():
                p.unlink()
                self._prune(p.parent, top)
        return iter(())

    @staticmethod
    def _prune(folder, stop):
        """Empty folders disappear, as 'folders' do in S3 when their last object is deleted."""
        while folder != stop and stop in folder.parents:
            try:
                folder.rmdir()
            except OSError:
                break
            folder = folder.parent
