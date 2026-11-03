#!/usr/bin/env python3
"""Minimal Python client for hf-upload-server."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

import httpx

TERMINAL = {"success", "partial", "failed", "cancelled"}


class HFUploadClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None, timeout: float = 600.0):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        return {"X-API-Key": self.api_key} if self.api_key else {}

    def _request(self, method: str, path: str, **kwargs) -> Any:
        kwargs.setdefault("timeout", self.timeout)
        with httpx.Client(headers=self._headers()) as client:
            resp = client.request(method, self.base_url + path, **kwargs)
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
            return resp.json()

    def upload_files(
        self,
        paths: list[str | Path],
        dir: str = "",
        repo_id: Optional[str] = None,
        repo_type: Optional[str] = None,
        revision: Optional[str] = None,
        encrypt: bool = False,
        password: Optional[str] = None,
        commit_message: Optional[str] = None,
    ) -> dict[str, Any]:
        files = []
        handles = []
        try:
            for path in paths:
                fh = open(path, "rb")
                handles.append(fh)
                files.append(("files", (Path(path).name, fh, "application/octet-stream")))
            data = {
                "dir": dir,
                "encrypt": "true" if encrypt else "false",
                "wait": "false",
            }
            if repo_id:
                data["repo_id"] = repo_id
            if repo_type:
                data["repo_type"] = repo_type
            if revision:
                data["revision"] = revision
            if password:
                data["password"] = password
            if commit_message:
                data["commit_message"] = commit_message
            return self._request("POST", "/upload/file", files=files, data=data)
        finally:
            for fh in handles:
                fh.close()

    def upload_urls(
        self,
        urls: list[str],
        dir: str = "",
        repo_id: Optional[str] = None,
        repo_type: Optional[str] = None,
        encrypt: bool = False,
        password: Optional[str] = None,
    ) -> dict[str, Any]:
        payload = {
            "urls": urls,
            "dir": dir,
            "encrypt": encrypt,
            "wait": False,
        }
        if repo_id:
            payload["repo_id"] = repo_id
        if repo_type:
            payload["repo_type"] = repo_type
        if password:
            payload["password"] = password
        return self._request("POST", "/upload/url", json=payload)

    def upload_stream(
        self,
        path: str | Path,
        filename: Optional[str] = None,
        dir: str = "",
        repo_id: Optional[str] = None,
        repo_type: Optional[str] = None,
        encrypt: bool = False,
        password: Optional[str] = None,
    ) -> dict[str, Any]:
        name = filename or Path(path).name
        params = {
            "filename": name,
            "dir": dir,
            "encrypt": "true" if encrypt else "false",
            "wait": "false",
        }
        if repo_id:
            params["repo_id"] = repo_id
        if repo_type:
            params["repo_type"] = repo_type
        if password:
            params["password"] = password
        with open(path, "rb") as fh:
            return self._request("POST", "/upload/stream", params=params, content=fh)

    def get_job(self, job_id: str) -> dict[str, Any]:
        return self._request("GET", f"/jobs/{job_id}")

    def wait_job(self, job_id: str, interval: float = 1.0, max_wait: Optional[float] = None) -> dict[str, Any]:
        deadline = time.monotonic() + max_wait if max_wait else None
        while True:
            job = self.get_job(job_id)
            if job["status"] in TERMINAL:
                return job
            if deadline and time.monotonic() > deadline:
                raise TimeoutError(f"job {job_id} still {job['status']}")
            time.sleep(interval)

    def queue(self) -> dict[str, Any]:
        return self._request("GET", "/queue")

    def metrics(self) -> str:
        with httpx.Client(headers=self._headers(), timeout=self.timeout) as client:
            resp = client.get(self.base_url + "/metrics")
            resp.raise_for_status()
            return resp.text


def print_job(job: dict[str, Any]) -> int:
    print(json.dumps(job, ensure_ascii=False, indent=2))
    status = job.get("status")
    if status == "success":
        return 0
    if status in {"failed", "partial"}:
        for item in job.get("errors", []):
            print(f"ERROR {item.get('path_in_repo')}: {item.get('error')}", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="hf-upload-server Python client")
    parser.add_argument("--base", default=os.environ.get("HF_UPLOAD_BASE", "http://127.0.0.1:8000"))
    parser.add_argument("--api-key", default=os.environ.get("HF_SERVER_API_KEY"))
    parser.add_argument("--repo-id", default=os.environ.get("HF_REPO_ID"))
    parser.add_argument("--repo-type", default=os.environ.get("HF_REPO_TYPE", "dataset"))
    parser.add_argument("--dir", default="")
    parser.add_argument("--encrypt", action="store_true")
    parser.add_argument("--password")
    parser.add_argument("--no-wait", action="store_true", help="提交后立即返回 job id")
    sub = parser.add_subparsers(dest="command", required=True)

    p_file = sub.add_parser("file", help="上传一个或多个本地文件")
    p_file.add_argument("paths", nargs="+")

    p_url = sub.add_parser("url", help="上传一个或多个远程 URL")
    p_url.add_argument("urls", nargs="+")

    p_stream = sub.add_parser("stream", help="以原始流上传单个文件")
    p_stream.add_argument("path")

    p_job = sub.add_parser("job", help="查看任务")
    p_job.add_argument("job_id")

    sub.add_parser("queue", help="查看队列")
    sub.add_parser("metrics", help="查看 Prometheus 指标")

    args = parser.parse_args()
    client = HFUploadClient(args.base, args.api_key)

    if args.command == "file":
        job = client.upload_files(
            args.paths, dir=args.dir, repo_id=args.repo_id, repo_type=args.repo_type,
            encrypt=args.encrypt, password=args.password,
        )
    elif args.command == "url":
        job = client.upload_urls(
            args.urls, dir=args.dir, repo_id=args.repo_id, repo_type=args.repo_type,
            encrypt=args.encrypt, password=args.password,
        )
    elif args.command == "stream":
        job = client.upload_stream(
            args.path, dir=args.dir, repo_id=args.repo_id, repo_type=args.repo_type,
            encrypt=args.encrypt, password=args.password,
        )
    elif args.command == "job":
        return print_job(client.get_job(args.job_id))
    elif args.command == "queue":
        print(json.dumps(client.queue(), ensure_ascii=False, indent=2))
        return 0
    else:
        print(client.metrics())
        return 0

    job_id = job["id"]
    if args.no_wait:
        print(json.dumps(job, ensure_ascii=False, indent=2))
        return 0
    return print_job(client.wait_job(job_id))


if __name__ == "__main__":
    raise SystemExit(main())
