from __future__ import annotations

import hashlib
import json
import threading
import time
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import hf_upload_server as srv
from hf_push import decrypt_file

H = {"X-API-Key": "test-secret"}


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass


def make_settings(tmp_path: Path, **overrides) -> srv.Settings:
    temp_dir = tmp_path / "temp"
    temp_dir.mkdir(exist_ok=True)
    kwargs = dict(
        repo_id=None,
        repo_type="dataset",
        revision="main",
        token="hf_dummy",
        default_dir="uploads",
        auto_create_repo=False,
        max_concurrent_uploads=4,
        max_concurrent_commits=2,
        max_concurrent_downloads=3,
        temp_dir=str(temp_dir),
        keep_temp=False,
        api_key="test-secret",
        allow_private_urls=True,
        job_history_size=100,
        sse_interval=0.05,
        encrypt_default_password="123456",
    )
    kwargs.update(overrides)
    settings = srv.Settings(**kwargs)
    settings.finalize()
    return settings


@pytest.fixture()
def data_dir(tmp_path: Path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    (d / "a.txt").write_bytes(b"alpha content\n")
    (d / "b.txt").write_bytes(b"beta content\n")
    (d / "c.txt").write_bytes(b"gamma from http\n")
    (d / "d.txt").write_bytes(b"delta stream body\n")
    (d / "e.txt").write_bytes(b"epsilon encrypted\n")
    return d


@pytest.fixture()
def url_server(data_dir: Path):
    handler = partial(QuietHandler, directory=str(data_dir))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


@pytest.fixture()
def fake_client(tmp_path: Path, monkeypatch):
    calls = []

    def fake_commit(self, target, files, commit_message):
        record = {
            "repo": target.repo_id,
            "paths": [ft.path_in_repo for ft in files],
            "sizes": [Path(ft.upload_path).stat().st_size for ft in files],
            "decrypted": {},
        }
        for ft in files:
            if ft.path_in_repo.endswith(".aes"):
                out = tmp_path / f"dec-{time.time_ns()}.txt"
                decrypt_file(Path(ft.upload_path), out, "testpass")
                record["decrypted"][ft.path_in_repo] = out.read_bytes()
                out.unlink()
        calls.append(record)
        return SimpleNamespace(oid="mock-commit")

    monkeypatch.setattr(srv.JobManager, "_commit_sync", fake_commit)
    settings = make_settings(tmp_path)
    app = srv.create_app(settings)
    with TestClient(app) as client:
        yield client, calls, Path(settings.temp_dir)


def test_auth_config_health_errors_dashboard(fake_client):
    client, _, _ = fake_client
    assert client.get("/config").status_code == 401
    cfg = client.get("/config", headers=H).json()
    assert cfg["token_configured"] is True
    assert "token" not in cfg
    assert "hf_dummy" not in json.dumps(cfg)
    assert client.get("/health").status_code == 200
    assert {"http_status", "job_status", "file_status", "common_hf_errors"} <= set(client.get("/errors").json())
    dash = client.get("/dashboard")
    assert dash.status_code == 200 and "队列监控" in dash.text


def test_multipart_batch_and_dedupe(fake_client, data_dir):
    client, calls, _ = fake_client
    files = [
        ("files", ("a.txt", (data_dir / "a.txt").read_bytes(), "text/plain")),
        ("files", ("a.txt", (data_dir / "b.txt").read_bytes(), "text/plain")),
    ]
    r = client.post(
        "/upload/file",
        headers=H,
        files=files,
        data={"repo_id": "test/fake", "dir": "incoming", "wait": "true"},
    )
    assert r.status_code == 200, r.text
    job = r.json()
    assert job["status"] == "success"
    assert len(calls) == 1
    assert calls[0]["paths"] == ["incoming/a.txt", "incoming/a-1.txt"]
    assert job["progress"]["percent"] == 100.0
    assert job["progress"]["completed_files"] == 2


def test_url_upload(fake_client, data_dir, url_server):
    client, calls, _ = fake_client
    r = client.post(
        "/upload/url",
        headers=H,
        json={"repo_id": "test/fake", "urls": [f"{url_server}/c.txt"], "dir": "incoming", "wait": True},
    )
    assert r.status_code == 200, r.text
    job = r.json()
    assert job["status"] == "success"
    assert calls[-1]["paths"] == ["incoming/c.txt"]
    assert job["files"][0]["sha256"] == hashlib.sha256((data_dir / "c.txt").read_bytes()).hexdigest()


def test_stream_upload(fake_client, data_dir):
    client, calls, _ = fake_client
    r = client.post(
        "/upload/stream",
        headers=H,
        params={"repo_id": "test/fake", "filename": "d.txt", "dir": "incoming", "wait": "true"},
        content=(data_dir / "d.txt").read_bytes(),
    )
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "success"
    assert calls[-1]["paths"] == ["incoming/d.txt"]


def test_encrypted_upload(fake_client, data_dir):
    client, calls, _ = fake_client
    files = [("files", ("e.txt", (data_dir / "e.txt").read_bytes(), "text/plain"))]
    r = client.post(
        "/upload/file",
        headers=H,
        files=files,
        data={"repo_id": "test/fake", "dir": "enc", "encrypt": "true", "password": "testpass", "wait": "true"},
    )
    assert r.status_code == 200, r.text
    job = r.json()
    assert job["status"] == "success"
    assert calls[-1]["paths"] == ["enc/e.txt.aes"]
    assert calls[-1]["decrypted"]["enc/e.txt.aes"] == (data_dir / "e.txt").read_bytes()
    assert job["files"][0]["sha256"] == hashlib.sha256((data_dir / "e.txt").read_bytes()).hexdigest()


def test_async_polling_and_sse(fake_client):
    client, _, _ = fake_client
    r = client.post(
        "/upload/file",
        headers=H,
        files=[("files", ("async.txt", b"async\n", "text/plain"))],
        data={"repo_id": "test/fake", "dir": "incoming", "wait": "false"},
    )
    assert r.status_code == 202, r.text
    jid = r.json()["id"]
    job = {}
    for _ in range(200):
        job = client.get(f"/jobs/{jid}", headers=H).json()
        if job["status"] in srv.TERMINAL:
            break
        time.sleep(0.05)
    assert job["status"] == "success", job

    with client.stream("GET", f"/jobs/{jid}/events", headers=H) as resp:
        assert resp.status_code == 200
        first = None
        for line in resp.iter_lines():
            if line.startswith("data:"):
                first = json.loads(line[5:].strip())
                break
    assert first and first["id"] == jid


def test_queue_and_metrics(fake_client, data_dir):
    client, _, _ = fake_client
    r = client.post(
        "/upload/file",
        headers=H,
        files=[("files", ("a.txt", (data_dir / "a.txt").read_bytes(), "text/plain"))],
        data={"repo_id": "test/fake", "wait": "true"},
    )
    assert r.json()["status"] == "success"
    q = client.get("/queue", headers=H).json()
    assert q["jobs"]["success"] >= 1
    assert q["concurrency"]["commit_active"] == 0
    assert q["totals"]["uploaded_files"] >= 1
    metrics = client.get("/metrics", headers=H).text
    assert "hf_upload_files_uploaded_total" in metrics
    assert "hf_upload_active_commits" in metrics
    assert client.get("/jobs", headers=H, params={"limit": 10}).json()["total"] >= 1


def test_validation(fake_client, data_dir):
    client, calls, _ = fake_client
    cases = [
        ({"repo_id": "test/fake", "dir": "../evil", "wait": "true"}, 400),
        ({"repo_id": "bad-id", "wait": "true"}, 400),
        ({"repo_id": "test/fake", "token": "hf_other", "wait": "true"}, 403),
    ]
    for data, expected in cases:
        r = client.post(
            "/upload/file",
            headers=H,
            files=[("files", ("x.txt", b"x", "text/plain"))],
            data=data,
        )
        assert r.status_code == expected, (data, r.text)
    assert calls == []


def test_ssrf_guard(tmp_path, url_server):
    settings = make_settings(tmp_path, allow_private_urls=False)
    app = srv.create_app(settings)
    with TestClient(app) as client:
        r = client.post(
            "/upload/url",
            headers=H,
            json={"repo_id": "test/fake", "urls": [f"{url_server}/c.txt"], "dir": "incoming", "wait": True},
        )
        assert r.status_code == 400
        assert "内网" in r.text


def test_max_file_size(tmp_path):
    settings = make_settings(tmp_path, max_file_size=5)
    app = srv.create_app(settings)
    with TestClient(app) as client:
        r = client.post(
            "/upload/file",
            headers=H,
            files=[("files", ("big.txt", b"1234567890", "text/plain"))],
            data={"repo_id": "test/fake", "wait": "true"},
        )
        assert r.status_code == 413, r.text


def test_commit_failure_reports_errors(tmp_path, data_dir, monkeypatch):
    def boom(self, target, files, commit_message):
        raise RuntimeError("mock commit failure")

    monkeypatch.setattr(srv.JobManager, "_commit_sync", boom)
    settings = make_settings(tmp_path)
    app = srv.create_app(settings)
    with TestClient(app) as client:
        r = client.post(
            "/upload/file",
            headers=H,
            files=[("files", ("a.txt", (data_dir / "a.txt").read_bytes(), "text/plain"))],
            data={"repo_id": "test/fake", "dir": "incoming", "wait": "true"},
        )
        assert r.status_code == 200, r.text
        job = r.json()
        assert job["status"] == "failed"
        assert "mock commit failure" in job["files"][0]["error"]
        assert job["errors"][0]["path_in_repo"] == "incoming/a.txt"
        q = client.get("/queue", headers=H).json()
        assert q["jobs"]["failed"] == 1
        assert q["totals"]["failed_files"] == 1


def test_temp_cleanup(fake_client, data_dir):
    client, _, temp_dir = fake_client
    r = client.post(
        "/upload/file",
        headers=H,
        files=[("files", ("a.txt", (data_dir / "a.txt").read_bytes(), "text/plain"))],
        data={"repo_id": "test/fake", "wait": "true"},
    )
    assert r.json()["status"] == "success"
    assert list(temp_dir.iterdir()) == []
