"""The folder backend behaves like S3 for everything the dashboard does: test plans, run summaries, reports, deletion."""
import io
import json
import zipfile

import pytest

import config
import storage
from app import create_app
from folder_store import FolderClient


@pytest.fixture
def folder(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "STORAGE", "folder")
    monkeypatch.setattr(config, "STORAGE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "RESULTS_URL", "s3://perf-results/runs")
    monkeypatch.setattr(config, "PLANS_URL", "s3://perf-results/test-plans")
    monkeypatch.setattr(storage, "_client", None)
    monkeypatch.setattr(storage, "_cache", {})
    return tmp_path


def test_s3_like_listing_reading_and_writing(folder):
    c = FolderClient(folder)
    c.put_object("b", "runs/r1/logs/worker-0.json", io.BytesIO(b'{"a":1}'), 7)
    c.put_object("b", "runs/r1/report/summary.json", io.BytesIO(b"{}"), 2)
    c.put_object("b", "runs/r2/x.txt", io.BytesIO(b"x"), 1)
    assert [o.object_name for o in c.list_objects("b", "runs/", recursive=False)] == ["runs/r1/", "runs/r2/"]
    assert all(o.is_dir for o in c.list_objects("b", "runs/"))
    assert [o.object_name for o in c.list_objects("b", "runs/r1/", recursive=True)] == ["runs/r1/logs/worker-0.json", "runs/r1/report/summary.json"]
    assert [o.object_name for o in c.list_objects("b", "runs/r", recursive=True)][-1] == "runs/r2/x.txt"   # plain prefix, as S3
    assert c.get_object("b", "runs/r1/logs/worker-0.json").read() == b'{"a":1}'
    assert b"".join(c.get_object("b", "runs/r1/logs/worker-0.json").stream(2)) == b'{"a":1}'
    with pytest.raises(Exception) as e:
        c.get_object("b", "runs/nope.json")
    assert e.value.code == "NoSuchKey"
    assert list(c.list_objects("missing-bucket", "")) == []


def test_keys_cannot_leave_the_folder(folder):
    c = FolderClient(folder / "data")
    for key in ["../outside.txt", "a/../../outside.txt"]:
        with pytest.raises(ValueError):
            c.put_object("b", key, io.BytesIO(b"x"), 1)
    assert not (folder / "outside.txt").exists()


def test_deleting_a_prefix_removes_files_and_empty_folders(folder):
    c = FolderClient(folder)
    c.put_object("perf-results", "runs/a-1/logs/w.log", io.BytesIO(b"x"), 1)
    c.put_object("perf-results", "runs/a-11/logs/w.log", io.BytesIO(b"x"), 1)
    assert storage.delete_run("a-1") == 1
    assert not (folder / "perf-results/runs/a-1").exists()           # no empty folders left behind
    assert (folder / "perf-results/runs/a-11/logs/w.log").exists()   # only that run


def test_plans_summaries_and_reports_work_on_a_folder(folder):
    app = create_app().test_client()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("suite/plans/p.yaml", "name: p\nscenarios: []\n")
        z.writestr("suite/data/users.csv", "u\n1\n")
    r = app.post("/api/plans/demo/upload", data={"files": [(io.BytesIO(buf.getvalue()), "s.zip")]}, content_type="multipart/form-data")
    assert r.status_code == 200, r.get_json()
    assert (folder / "perf-results/test-plans/demo/plans/p.yaml").is_file()
    files = {f["path"]: f for f in app.get("/api/plans/demo/files").get_json()["files"]}
    assert files["plans/p.yaml"]["plan"] and set(files) == {"plans/p.yaml", "data/users.csv"}

    run = folder / "perf-results/runs/t-1234abcd"
    (run / "report/merged").mkdir(parents=True)
    (run / "report/summary.json").write_text(json.dumps({"start": "2026-11-25T10:00:00Z", "total": {"count": 5}}))
    (run / "report/merged/index.html").write_text("<h1>report</h1>")
    runs = app.get("/api/runs").get_json()
    assert runs[0]["folder"] == "t-1234abcd" and runs[0]["summary"]["total"]["count"] == 5
    page = app.get("/reports/" + runs[0]["report"])
    assert page.status_code == 200 and page.data == b"<h1>report</h1>" and page.content_type.startswith("text/html")
    assert app.get("/reports/runs/../../secret.txt").status_code == 404
