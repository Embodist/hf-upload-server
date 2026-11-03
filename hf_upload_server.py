#!/usr/bin/env python3
"""FastAPI 异步上传服务：外部文件/URL -> Hugging Face 仓库。

主要能力:
  POST /upload/file     multipart 表单上传（支持多文件）
  POST /upload/url      JSON 提交一个或多个远程 URL
  POST /upload/stream   原始请求体流式上传（高性能，配合 X-Filename）
  GET  /jobs, /jobs/{id}                 任务列表/详情
  GET  /jobs/{id}/events                 SSE 实时进度
  GET  /queue                            队列与并发监控
  GET  /metrics                          Prometheus 指标
  GET  /files, /whoami, /config          仓库文件/身份/配置
  POST /repos                            创建仓库

仓库配置优先取启动参数/配置文件，单个请求可通过 repo_id/repo_type/revision 等覆盖。
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import ipaddress
import json
import logging
import os
import posixpath
import re
import secrets
import shutil
import socket
import sys
import tempfile
import time
import uuid
from collections import Counter
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import unquote, urljoin, urlparse, urlunparse

import aiofiles
import httpx
import uvicorn
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse
from huggingface_hub import CommitOperationAdd, HfApi, get_token
from huggingface_hub.errors import HfHubHTTPError
from pydantic import BaseModel, Field

from hf_push import encrypt_file

logger = logging.getLogger("hf_upload_server")

DEFAULT_CONFIG_PATH = Path(__file__).with_name("server_config.json")
REPO_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$")
REVISION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")
REPO_TYPES = {"dataset", "model", "space"}
TERMINAL = {"success", "partial", "failed", "cancelled"}
CHUNK_SIZE = 1024 * 1024


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------


@dataclass
class Settings:
    repo_id: Optional[str] = None
    repo_type: str = "dataset"
    revision: str = "main"
    token: Optional[str] = None
    default_dir: str = ""
    private: bool = True
    auto_create_repo: bool = True
    max_concurrent_uploads: int = 4  # 单次 commit 内的上传线程数
    max_concurrent_commits: int = 2  # 同时进行的 commit 数
    max_concurrent_downloads: int = 8
    max_file_size: int = 0  # 0 = 不限制（字节）
    temp_dir: Optional[str] = None
    keep_temp: bool = False
    encrypt_default_password: str = "123456"
    api_key: Optional[str] = None
    allow_token_override: bool = False
    allow_private_urls: bool = False
    allowed_hosts: list[str] = field(default_factory=list)
    cors_origins: list[str] = field(default_factory=list)
    job_history_size: int = 500
    sse_interval: float = 1.0
    connect_timeout: float = 15.0
    read_timeout: float = 120.0
    download_retries: int = 3

    @classmethod
    def load(cls, path: Path, overrides: dict[str, Any]) -> "Settings":
        raw: dict[str, Any] = {}
        if path and path.exists():
            raw = json.loads(path.read_text(encoding="utf-8"))
        elif path:
            logger.warning("配置文件不存在，使用默认配置: %s", path)
        raw = expand_env(raw)
        allowed = {f.name for f in fields(cls)}
        unknown = sorted(set(raw) - allowed)
        if unknown:
            logger.warning("忽略未知配置项: %s", unknown)
        obj = cls(**{k: v for k, v in raw.items() if k in allowed})
        for key, value in overrides.items():
            if value is not None:
                setattr(obj, key, value)
        obj.finalize()
        return obj

    def finalize(self) -> None:
        if self.repo_type not in REPO_TYPES:
            raise ValueError(f"repo_type 必须是 {sorted(REPO_TYPES)} 之一")
        if self.repo_id and not REPO_ID_RE.match(self.repo_id):
            raise ValueError(f"repo_id 格式非法: {self.repo_id}")
        if not REVISION_RE.match(self.revision or "") or ".." in self.revision:
            raise ValueError(f"revision 非法: {self.revision}")
        self.max_concurrent_uploads = max(1, int(self.max_concurrent_uploads))
        self.max_concurrent_commits = max(1, int(self.max_concurrent_commits))
        self.max_concurrent_downloads = max(1, int(self.max_concurrent_downloads))
        self.job_history_size = max(10, int(self.job_history_size))
        self.max_file_size = max(0, int(self.max_file_size))
        self.token = (
            self.token
            or os.environ.get("HF_TOKEN")
            or os.environ.get("HUGGING_FACE_HUB_TOKEN")
            or get_token()
        )
        self.api_key = self.api_key or None

    def public_dict(self) -> dict[str, Any]:
        return {
            "repo_id": self.repo_id,
            "repo_type": self.repo_type,
            "revision": self.revision,
            "default_dir": self.default_dir,
            "private_default": self.private,
            "auto_create_repo": self.auto_create_repo,
            "max_concurrent_uploads": self.max_concurrent_uploads,
            "max_concurrent_commits": self.max_concurrent_commits,
            "max_concurrent_downloads": self.max_concurrent_downloads,
            "max_file_size": self.max_file_size,
            "temp_dir": self.temp_dir,
            "keep_temp": self.keep_temp,
            "token_configured": bool(self.token),
            "api_key_required": bool(self.api_key),
            "allow_token_override": self.allow_token_override,
            "allow_private_urls": self.allow_private_urls,
            "allowed_hosts": self.allowed_hosts,
            "job_history_size": self.job_history_size,
        }


def expand_env(value: Any) -> Any:
    if isinstance(value, str):
        return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", lambda m: os.environ.get(m.group(1), ""), value)
    if isinstance(value, list):
        return [expand_env(v) for v in value]
    if isinstance(value, dict):
        return {k: expand_env(v) for k, v in value.items()}
    return value


# ---------------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------------


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _exc_message(exc: BaseException | None) -> str:
    if exc is None:
        return "未知错误"
    text = str(exc).strip()
    return text or exc.__class__.__name__


def redact_url(url: str) -> str:
    try:
        p = urlparse(url)
        host = p.hostname or ""
        if p.port:
            host += f":{p.port}"
        return urlunparse((p.scheme, host, p.path, "", "", ""))
    except Exception:
        return "<url>"


def filename_from_url(url: str) -> str:
    return unquote(posixpath.basename(urlparse(url).path)) or "download.bin"


def safe_filename(name: str | None) -> str:
    name = unquote(name or "").replace("\\", "/")
    name = posixpath.basename(name).strip()
    name = re.sub(r"[\x00-\x1f\x7f]", "", name)
    if name in ("", ".", ".."):
        raise HTTPException(400, "文件名无效")
    if len(name) > 255:
        raise HTTPException(400, "文件名过长")
    return name


def safe_dir(value: str | None) -> str:
    value = (value or "").replace("\\", "/").strip().strip("/")
    value = re.sub(r"[\x00-\x1f\x7f]", "", value)
    if not value:
        return ""
    normalized = posixpath.normpath(value)
    if normalized in ("", "."):
        return ""
    if normalized.startswith("/") or normalized == ".." or normalized.startswith("../"):
        raise HTTPException(400, "目录路径非法")
    if any(part in ("", "..") for part in normalized.split("/")):
        raise HTTPException(400, "目录路径非法")
    return normalized


def unique_name(name: str, used: set[str]) -> str:
    if name not in used:
        used.add(name)
        return name
    stem, ext = os.path.splitext(name)
    i = 1
    while f"{stem}-{i}{ext}" in used:
        i += 1
    candidate = f"{stem}-{i}{ext}"
    used.add(candidate)
    return candidate


def _ip_forbidden(ip: ipaddress._BaseAddress) -> bool:
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


async def validate_url(url: str, settings: Settings) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise HTTPException(400, f"仅支持 http/https URL: {url}")
    if not parsed.hostname:
        raise HTTPException(400, f"URL 缺少主机名: {url}")
    if settings.allowed_hosts and parsed.hostname not in settings.allowed_hosts:
        raise HTTPException(403, f"主机不在白名单: {parsed.hostname}")
    if settings.allow_private_urls:
        return
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        infos = await asyncio.to_thread(socket.getaddrinfo, parsed.hostname, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise HTTPException(400, f"无法解析域名 {parsed.hostname}: {exc}")
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if _ip_forbidden(ip):
            raise HTTPException(400, f"URL 指向内网/本地地址，已拒绝: {parsed.hostname}")


def _int_header(resp: httpx.Response, name: str) -> int:
    try:
        return int(resp.headers.get(name) or 0)
    except ValueError:
        return 0


# ---------------------------------------------------------------------------
# 任务模型
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Target:
    repo_id: str
    repo_type: str
    revision: str
    token: str
    private: bool
    auto_create: bool

    def public_dict(self) -> dict[str, Any]:
        return {"repo_id": self.repo_id, "repo_type": self.repo_type, "revision": self.revision}


@dataclass
class FileTask:
    source: str
    kind: str  # local | url
    filename: str
    path_in_repo: str
    temp_path: Optional[str] = None
    upload_path: Optional[str] = None
    size: int = 0
    downloaded_bytes: int = 0
    sha256: Optional[str] = None
    status: str = "pending"
    commit_id: Optional[str] = None
    error: Optional[str] = None

    def public_dict(self) -> dict[str, Any]:
        source = redact_url(self.source) if self.kind == "url" else self.filename
        return {
            "source": source,
            "kind": self.kind,
            "filename": self.filename,
            "path_in_repo": self.path_in_repo,
            "size": self.size,
            "downloaded_bytes": self.downloaded_bytes,
            "sha256": self.sha256,
            "status": self.status,
            "commit_id": self.commit_id,
            "error": self.error,
        }


@dataclass
class Job:
    id: str
    target: Target
    encrypt: bool
    password: Optional[str]
    commit_message: Optional[str]
    files: list[FileTask]
    temp_dir: Optional[str] = None
    status: str = "queued"
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    error: Optional[str] = None

    def touch(self) -> None:
        self.updated_at = now_iso()

    def public_dict(self) -> dict[str, Any]:
        weights = {
            "pending": 0.0,
            "downloading": 0.2,
            "saved": 0.4,
            "downloaded": 0.45,
            "encrypting": 0.6,
            "prepared": 0.75,
            "uploading": 0.9,
            "success": 1.0,
            "failed": 1.0,
            "cancelled": 1.0,
        }
        total = len(self.files)
        completed = sum(1 for f in self.files if f.status == "success")
        prepared = sum(1 for f in self.files if f.status in {"prepared", "uploading", "success"})
        failed = sum(1 for f in self.files if f.status in {"failed", "cancelled"})
        percent = sum(weights.get(f.status, 0.0) for f in self.files) * 100 / total if total else 0.0
        return {
            "id": self.id,
            "status": self.status,
            "repo": {
                **self.target.public_dict(),
                "encrypt": self.encrypt,
            },
            "commit_message": self.commit_message,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
            "progress": {
                "completed_files": completed,
                "prepared_files": prepared,
                "failed_files": failed,
                "total_files": total,
                "percent": round(percent, 1),
            },
            "errors": [
                {"path_in_repo": f.path_in_repo, "error": f.error}
                for f in self.files
                if f.error
            ],
            "files": [f.public_dict() for f in self.files],
        }


class JobManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.jobs: dict[str, Job] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        self.lock = asyncio.Lock()
        self.commit_sem = asyncio.Semaphore(settings.max_concurrent_commits)
        self.download_sem = asyncio.Semaphore(settings.max_concurrent_downloads)
        self.commit_slots = 0
        self.download_slots = 0
        self.known_repos: set[tuple[str, str]] = set()
        self.repo_lock = asyncio.Lock()
        self.client: Optional[httpx.AsyncClient] = None
        self.total_uploaded_files = 0
        self.total_failed_files = 0
        self.total_bytes_downloaded = 0
        self.total_bytes_uploaded = 0
        self.total_jobs_finished: Counter[str] = Counter()

    async def startup(self) -> None:
        self.client = httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=self.settings.max_concurrent_downloads * 2 + 8,
                max_keepalive_connections=self.settings.max_concurrent_downloads + 4,
            ),
            timeout=httpx.Timeout(
                connect=self.settings.connect_timeout,
                read=self.settings.read_timeout,
                write=60.0,
                pool=self.settings.connect_timeout,
            ),
            headers={"User-Agent": "hf-upload-server/1.0"},
        )

    async def shutdown(self) -> None:
        for task in list(self.tasks.values()):
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        if self.client is not None:
            await self.client.aclose()

    def new_temp_dir(self) -> Path:
        if self.settings.temp_dir:
            Path(self.settings.temp_dir).mkdir(parents=True, exist_ok=True)
        return Path(tempfile.mkdtemp(prefix="hfups-", dir=self.settings.temp_dir))

    def create_job(
        self,
        target: Target,
        encrypt: bool,
        password: Optional[str],
        commit_message: Optional[str],
        files: list[FileTask],
        temp_dir: Path,
    ) -> Job:
        return Job(
            id=uuid.uuid4().hex,
            target=target,
            encrypt=encrypt,
            password=password,
            commit_message=commit_message,
            files=files,
            temp_dir=str(temp_dir),
        )

    async def submit(self, job: Job) -> None:
        async with self.lock:
            self.jobs[job.id] = job
            task = asyncio.create_task(self._run(job), name=f"job-{job.id}")
            self.tasks[job.id] = task
        logger.info("job %s queued: %s files -> %s", job.id, len(job.files), job.target.repo_id)

    async def wait(self, job_id: str) -> Job:
        task = self.tasks.get(job_id)
        if task is not None:
            with suppress(asyncio.CancelledError):
                await task
        job = self.jobs.get(job_id)
        if job is None:
            raise HTTPException(404, "任务不存在")
        return job

    async def get(self, job_id: str) -> Optional[Job]:
        async with self.lock:
            return self.jobs.get(job_id)

    async def list_jobs(self, status_filter: Optional[str], limit: int, offset: int) -> dict[str, Any]:
        async with self.lock:
            jobs = sorted(self.jobs.values(), key=lambda j: j.created_at, reverse=True)
        if status_filter:
            jobs = [j for j in jobs if j.status == status_filter]
        total = len(jobs)
        page = jobs[offset : offset + limit]
        return {"total": total, "offset": offset, "limit": limit, "jobs": [j.public_dict() for j in page]}

    async def queue_snapshot(self) -> dict[str, Any]:
        async with self.lock:
            counts = Counter(j.status for j in self.jobs.values())
            queued_ids = [j.id for j in self.jobs.values() if j.status == "queued"]
            running_ids = [j.id for j in self.jobs.values() if j.status == "running"]
            return {
                "concurrency": {
                    "upload_threads_per_commit": self.settings.max_concurrent_uploads,
                    "commit_limit": self.settings.max_concurrent_commits,
                    "commit_active": self.commit_slots,
                    "download_limit": self.settings.max_concurrent_downloads,
                    "download_active": self.download_slots,
                },
                "jobs": {name: counts.get(name, 0) for name in ("queued", "running", "success", "partial", "failed", "cancelled")},
                "queued_job_ids": queued_ids,
                "running_job_ids": running_ids,
                "totals": {
                    "uploaded_files": self.total_uploaded_files,
                    "failed_files": self.total_failed_files,
                    "bytes_downloaded": self.total_bytes_downloaded,
                    "bytes_uploaded": self.total_bytes_uploaded,
                    "finished_jobs": dict(self.total_jobs_finished),
                },
            }

    async def metrics_text(self) -> str:
        snap = await self.queue_snapshot()
        lines = [
            "# HELP hf_upload_jobs Number of jobs by status",
            "# TYPE hf_upload_jobs gauge",
        ]
        for name, count in snap["jobs"].items():
            lines.append(f'hf_upload_jobs{{status="{name}"}} {count}')
        lines += [
            "# HELP hf_upload_active_commits Number of commits currently uploading",
            "# TYPE hf_upload_active_commits gauge",
            f"hf_upload_active_commits {snap['concurrency']['commit_active']}",
            "# HELP hf_upload_upload_threads_per_commit Upload threads used per commit",
            "# TYPE hf_upload_upload_threads_per_commit gauge",
            f"hf_upload_upload_threads_per_commit {snap['concurrency']['upload_threads_per_commit']}",
            "# HELP hf_upload_active_downloads Number of files currently downloading",
            "# TYPE hf_upload_active_downloads gauge",
            f"hf_upload_active_downloads {snap['concurrency']['download_active']}",
            "# HELP hf_upload_files_uploaded_total Files uploaded successfully",
            "# TYPE hf_upload_files_uploaded_total counter",
            f"hf_upload_files_uploaded_total {snap['totals']['uploaded_files']}",
            "# HELP hf_upload_files_failed_total Files failed",
            "# TYPE hf_upload_files_failed_total counter",
            f"hf_upload_files_failed_total {snap['totals']['failed_files']}",
            "# HELP hf_upload_bytes_downloaded_total Bytes downloaded from remote URLs",
            "# TYPE hf_upload_bytes_downloaded_total counter",
            f"hf_upload_bytes_downloaded_total {snap['totals']['bytes_downloaded']}",
            "# HELP hf_upload_bytes_uploaded_total Bytes uploaded to Hugging Face",
            "# TYPE hf_upload_bytes_uploaded_total counter",
            f"hf_upload_bytes_uploaded_total {snap['totals']['bytes_uploaded']}",
        ]
        return "\n".join(lines) + "\n"

    async def _evict_locked(self) -> None:
        overflow = len(self.jobs) - self.settings.job_history_size
        if overflow <= 0:
            return
        terminal = sorted(
            (j for j in self.jobs.values() if j.status in TERMINAL),
            key=lambda j: j.finished_at or j.created_at,
        )
        for job in terminal[:overflow]:
            self.jobs.pop(job.id, None)
            task = self.tasks.get(job.id)
            if task is not None and task.done():
                self.tasks.pop(job.id, None)

    async def _run(self, job: Job) -> None:
        job.status = "running"
        job.started_at = now_iso()
        job.touch()
        logger.info("job %s started", job.id)
        try:
            if job.target.auto_create:
                await self._ensure_repo(job.target)
            results = await asyncio.gather(
                *(self._prepare_file(job, ft) for ft in job.files),
                return_exceptions=True,
            )
            for ft, result in zip(job.files, results):
                if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                    if ft.status != "success":
                        ft.status = "failed"
                        ft.error = ft.error or _exc_message(result)

            prepared = [ft for ft in job.files if ft.status == "prepared"]
            if prepared:
                for ft in prepared:
                    ft.status = "uploading"
                try:
                    info = await self._commit_all(job, prepared)
                    commit_id = getattr(info, "oid", None)
                    for ft in prepared:
                        ft.commit_id = commit_id
                        ft.status = "success"
                    async with self.lock:
                        self.total_uploaded_files += len(prepared)
                        self.total_bytes_uploaded += sum(ft.size for ft in prepared)
                    logger.info("job %s committed %s file(s) as %s", job.id, len(prepared), commit_id)
                except asyncio.CancelledError:
                    for ft in prepared:
                        ft.status = "cancelled"
                    raise
                except Exception as exc:
                    message = _exc_message(exc)
                    for ft in prepared:
                        ft.status = "failed"
                        ft.error = message
                    async with self.lock:
                        self.total_failed_files += len(prepared)
                    logger.error("job %s commit failed: %s", job.id, message)

            failed = sum(1 for ft in job.files if ft.status != "success")
            if failed == 0:
                job.status = "success"
            elif failed == len(job.files):
                job.status = "failed"
            else:
                job.status = "partial"
        except asyncio.CancelledError:
            job.status = "cancelled"
            for ft in job.files:
                if ft.status != "success":
                    ft.status = "cancelled"
            raise
        except Exception as exc:
            job.status = "failed"
            job.error = _exc_message(exc)
            for ft in job.files:
                if ft.status != "success":
                    ft.status = "failed"
                    ft.error = ft.error or job.error
        finally:
            job.finished_at = now_iso()
            job.touch()
            async with self.lock:
                self.total_jobs_finished[job.status] += 1
                await self._evict_locked()
            if not self.settings.keep_temp and job.temp_dir:
                shutil.rmtree(job.temp_dir, ignore_errors=True)
            logger.info("job %s finished: %s", job.id, job.status)

    async def _ensure_repo(self, target: Target) -> None:
        key = (target.repo_id, target.repo_type)
        async with self.repo_lock:
            if key in self.known_repos:
                return
            api = HfApi(token=target.token)
            exists = await asyncio.to_thread(api.repo_exists, target.repo_id, repo_type=target.repo_type)
            if not exists:
                await asyncio.to_thread(
                    api.create_repo,
                    repo_id=target.repo_id,
                    repo_type=target.repo_type,
                    private=target.private,
                    exist_ok=True,
                )
                logger.info("created repo %s (%s)", target.repo_id, target.repo_type)
            self.known_repos.add(key)

    async def _prepare_file(self, job: Job, ft: FileTask) -> None:
        try:
            if ft.kind == "url":
                local = await self._download(job, ft)
            else:
                local = Path(ft.temp_path or "")
                if not local.is_file():
                    raise RuntimeError("临时文件不存在")
                ft.status = "saved"

            if job.encrypt:
                ft.status = "encrypting"
                encrypted = Path(job.temp_dir or tempfile.gettempdir()) / f"enc-{uuid.uuid4().hex}.aes"
                ft.sha256 = await asyncio.to_thread(encrypt_file, local, encrypted, job.password)
                ft.upload_path = str(encrypted)
            else:
                ft.upload_path = str(local)
            ft.status = "prepared"
        except asyncio.CancelledError:
            ft.status = "cancelled"
            raise
        except Exception as exc:
            ft.status = "failed"
            ft.error = _exc_message(exc)
            async with self.lock:
                self.total_failed_files += 1
            raise

    async def _commit_all(self, job: Job, files: list[FileTask]):
        async with self.commit_sem:
            async with self.lock:
                self.commit_slots += 1
            try:
                return await asyncio.to_thread(self._commit_sync, job.target, files, job.commit_message)
            finally:
                async with self.lock:
                    self.commit_slots -= 1

    def _commit_sync(self, target: Target, files: list[FileTask], commit_message: Optional[str]):
        operations = [
            CommitOperationAdd(path_in_repo=ft.path_in_repo, path_or_fileobj=ft.upload_path)
            for ft in files
        ]
        api = HfApi(token=target.token)
        attempts = 3
        for attempt in range(attempts):
            try:
                return api.create_commit(
                    repo_id=target.repo_id,
                    operations=operations,
                    commit_message=commit_message or f"Upload {len(operations)} file(s)",
                    repo_type=target.repo_type,
                    revision=target.revision,
                    num_threads=min(self.settings.max_concurrent_uploads, max(1, len(operations))),
                )
            except HfHubHTTPError as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status not in {409, 429, 500, 502, 503, 504} or attempt + 1 >= attempts:
                    raise
                time.sleep(min(2 ** attempt, 8))

    async def _download(self, job: Job, ft: FileTask) -> Path:
        ft.status = "downloading"
        assert job.temp_dir is not None
        dest = Path(job.temp_dir) / f"{uuid.uuid4().hex}.part"
        last_exc: Optional[BaseException] = None
        attempts = max(1, self.settings.download_retries)
        for attempt in range(attempts):
            if dest.exists():
                dest.unlink()
            try:
                async with self.download_sem:
                    async with self.lock:
                        self.download_slots += 1
                    try:
                        size, sha = await self._stream_download(ft.source, dest, ft)
                    finally:
                        async with self.lock:
                            self.download_slots -= 1
                ft.size = size
                ft.sha256 = sha
                ft.status = "downloaded"
                async with self.lock:
                    self.total_bytes_downloaded += size
                return dest
            except HTTPException:
                raise
            except Exception as exc:
                last_exc = exc
                dest.unlink(missing_ok=True)
                if attempt + 1 < attempts:
                    await asyncio.sleep(min(2 ** attempt, 10))
        raise RuntimeError(f"下载失败: {_exc_message(last_exc)}")

    async def _stream_download(self, url: str, dest: Path, ft: FileTask) -> tuple[int, str]:
        assert self.client is not None
        current = url
        for _ in range(6):
            await validate_url(current, self.settings)
            async with self.client.stream("GET", current, follow_redirects=False) as resp:
                if resp.is_redirect:
                    location = resp.headers.get("location")
                    if not location:
                        raise RuntimeError("重定向缺少 Location")
                    current = urljoin(current, location)
                    continue
                resp.raise_for_status()
                total = _int_header(resp, "content-length")
                if self.settings.max_file_size and total > self.settings.max_file_size:
                    raise HTTPException(413, f"远程文件超过上限 {self.settings.max_file_size} 字节")
                size = 0
                digest = hashlib.sha256()
                async with aiofiles.open(dest, "wb") as fo:
                    async for chunk in resp.aiter_bytes(CHUNK_SIZE):
                        size += len(chunk)
                        if self.settings.max_file_size and size > self.settings.max_file_size:
                            raise HTTPException(413, f"远程文件超过上限 {self.settings.max_file_size} 字节")
                        digest.update(chunk)
                        await fo.write(chunk)
                        ft.downloaded_bytes = size
                return size, digest.hexdigest()
        raise RuntimeError("重定向次数过多")


# ---------------------------------------------------------------------------
# 请求模型
# ---------------------------------------------------------------------------


class UrlUploadRequest(BaseModel):
    urls: list[str] = Field(..., min_length=1, max_length=128)
    dir: str = ""
    filename: Optional[str] = None  # 仅在单个 URL 时生效
    repo_id: Optional[str] = None
    repo_type: Optional[str] = None
    revision: Optional[str] = None
    private: Optional[bool] = None
    encrypt: bool = False
    password: Optional[str] = None
    commit_message: Optional[str] = None
    wait: bool = False
    token: Optional[str] = None


class CreateRepoRequest(BaseModel):
    repo_id: str
    repo_type: str = "dataset"
    private: bool = True
    token: Optional[str] = None


DASHBOARD_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>HF Upload Server</title>
<style>
  :root { --bg:#0f172a; --panel:#1e293b; --muted:#94a3b8; --text:#e2e8f0; --ok:#22c55e; --warn:#f59e0b; --err:#ef4444; --run:#38bdf8; }
  * { box-sizing:border-box; }
  body { margin:0; padding:24px; background:var(--bg); color:var(--text); font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; }
  h1 { font-size:20px; margin:0 0 16px; }
  h2 { font-size:15px; margin:24px 0 10px; color:var(--muted); }
  .bar { display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-bottom:16px; }
  input { background:var(--panel); color:var(--text); border:1px solid #334155; border-radius:6px; padding:8px 10px; min-width:280px; }
  button { background:#2563eb; color:#fff; border:0; border-radius:6px; padding:8px 14px; cursor:pointer; }
  .msg { color:var(--err); font-size:12px; }
  .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(130px,1fr)); gap:10px; }
  .card { background:var(--panel); border:1px solid #273449; border-radius:8px; padding:12px; }
  .card .k { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.06em; }
  .card .v { font-size:24px; margin-top:6px; }
  table { width:100%; border-collapse:collapse; background:var(--panel); border-radius:8px; overflow:hidden; }
  th,td { text-align:left; padding:8px 10px; border-bottom:1px solid #273449; font-size:12px; }
  th { color:var(--muted); font-weight:600; }
  tr.row { cursor:pointer; } tr.row:hover { background:#243248; }
  .st { font-weight:700; }
  .st-success { color:var(--ok); } .st-failed { color:var(--err); } .st-partial,.st-cancelled { color:var(--warn); }
  .st-running { color:var(--run); } .st-queued { color:var(--muted); }
  pre { background:var(--panel); border:1px solid #273449; border-radius:8px; padding:12px; overflow:auto; font-size:12px; max-height:420px; }
  .muted { color:var(--muted); }
</style>
</head>
<body>
<h1>HF Upload Server <span class="muted">队列监控</span></h1>
<div class="bar">
  <input id="key" type="password" placeholder="X-API-Key（服务端未配置可留空）">
  <button id="save">保存 Key</button>
  <button id="reload">立即刷新</button>
  <span class="msg" id="msg"></span>
</div>
<div class="cards" id="cards"></div>
<h2>最近任务（点击查看详情）</h2>
<table>
  <thead><tr><th>ID</th><th>状态</th><th>仓库</th><th>进度</th><th>文件</th><th>错误</th><th>创建时间</th></tr></thead>
  <tbody id="jobs"></tbody>
</table>
<h2>任务详情</h2>
<pre id="detail" class="muted">点击上方任务查看详情</pre>
<script>
const $ = (id) => document.getElementById(id);
const keyInput = $("key");
keyInput.value = localStorage.getItem("hf_upload_api_key") || "";
let selected = null;
let jobIndex = {};

function esc(v) {
  return String(v ?? "").replace(/[&<>"']/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
}
function fmtBytes(n) {
  n = Number(n || 0);
  const units = ["B","KB","MB","GB","TB"];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return (i === 0 ? Math.round(n) : n.toFixed(2)) + " " + units[i];
}
async function api(path) {
  const headers = {};
  const key = keyInput.value.trim();
  if (key) headers["X-API-Key"] = key;
  const resp = await fetch(path, { headers });
  if (!resp.ok) throw new Error(resp.status + " " + (await resp.text()).slice(0, 300));
  return resp.json();
}
function statusClass(s) { return "st st-" + s; }
function renderCards(q) {
  const c = q.concurrency || {};
  const j = q.jobs || {};
  const t = q.totals || {};
  const cards = [
    ["排队", j.queued || 0],
    ["运行中", j.running || 0],
    ["成功", j.success || 0],
    ["失败", j.failed || 0],
    ["上传中 commit", c.commit_active || 0],
    ["并行 commit", c.commit_limit || 0],
    ["下载中", c.download_active || 0],
    ["已上传文件", t.uploaded_files || 0],
    ["下载字节", fmtBytes(t.bytes_downloaded)],
    ["上传字节", fmtBytes(t.bytes_uploaded)],
  ];
  $("cards").innerHTML = cards.map(([k, v]) =>
    '<div class="card"><div class="k">' + esc(k) + '</div><div class="v">' + esc(v) + '</div></div>'
  ).join("");
}
function renderJobs(data) {
  jobIndex = {};
  const rows = (data.jobs || []).map((j) => {
    jobIndex[j.id] = j;
    const errs = (j.errors || []).length;
    return '<tr class="row" data-id="' + esc(j.id) + '">' +
      '<td>' + esc(j.id.slice(0, 8)) + '</td>' +
      '<td class="' + statusClass(j.status) + '">' + esc(j.status) + '</td>' +
      '<td>' + esc(j.repo ? j.repo.repo_id : "") + '</td>' +
      '<td>' + esc(j.progress ? j.progress.percent : 0) + '%</td>' +
      '<td>' + esc(j.progress ? j.progress.completed_files + "/" + j.progress.total_files : "") + '</td>' +
      '<td>' + (errs ? '<span class="st-failed">' + errs + '</span>' : '0') + '</td>' +
      '<td>' + esc(j.created_at) + '</td></tr>';
  });
  $("jobs").innerHTML = rows.join("") || '<tr><td colspan="7" class="muted">暂无任务</td></tr>';
  document.querySelectorAll("tr.row").forEach((tr) => {
    tr.addEventListener("click", () => showJob(tr.dataset.id));
  });
}
async function showJob(id) {
  selected = id;
  $("detail").textContent = "加载中…";
  try {
    const job = await api("/jobs/" + encodeURIComponent(id));
    $("detail").textContent = JSON.stringify(job, null, 2);
  } catch (e) {
    $("detail").textContent = String(e);
  }
}
async function refresh() {
  try {
    const [q, jobs] = await Promise.all([api("/queue"), api("/jobs?limit=20")]);
    renderCards(q);
    renderJobs(jobs);
    $("msg").textContent = "";
    if (selected) {
      try { const job = await api("/jobs/" + encodeURIComponent(selected)); $("detail").textContent = JSON.stringify(job, null, 2); } catch (_) {}
    }
  } catch (e) {
    $("msg").textContent = String(e);
  }
}
$("save").addEventListener("click", () => {
  localStorage.setItem("hf_upload_api_key", keyInput.value.trim());
  $("msg").textContent = "已保存";
  refresh();
});
$("reload").addEventListener("click", refresh);
setInterval(refresh, 3000);
refresh();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# 应用
# ---------------------------------------------------------------------------


def create_app(settings: Settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        manager = JobManager(settings)
        await manager.startup()
        app.state.manager = manager
        if not settings.api_key:
            logger.warning("未配置 api_key，上传接口无鉴权；公网部署请务必设置")
        if settings.repo_id and settings.token:
            try:
                info = await asyncio.to_thread(HfApi(token=settings.token).whoami)
                logger.info("HF identity: %s (%s)", info.get("name"), info.get("type", "user"))
            except Exception as exc:
                logger.warning("无法验证 HF token: %s", _exc_message(exc))
        yield
        await manager.shutdown()

    app = FastAPI(
        title="HF Async Upload Server",
        version="1.0.0",
        description="外部文件/URL 异步上传到 Hugging Face 仓库，支持任务队列与监控。",
        lifespan=lifespan,
    )
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    def manager_of(request: Request) -> JobManager:
        return request.app.state.manager

    async def require_auth(
        x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
        authorization: Optional[str] = Header(None),
    ) -> None:
        if not settings.api_key:
            return
        supplied = x_api_key
        if not supplied and authorization and authorization.lower().startswith("bearer "):
            supplied = authorization[7:].strip()
        if not supplied or not secrets.compare_digest(supplied, settings.api_key):
            raise HTTPException(401, "无效的 API Key", headers={"WWW-Authenticate": "Bearer"})

    def resolve_target(
        repo_id: Optional[str],
        repo_type: Optional[str],
        revision: Optional[str],
        private: Optional[bool],
        token: Optional[str],
    ) -> Target:
        rid = repo_id or settings.repo_id
        if not rid:
            raise HTTPException(400, "未指定 repo_id，且服务器未配置默认仓库")
        if not REPO_ID_RE.match(rid):
            raise HTTPException(400, f"repo_id 格式非法: {rid}")
        rt = repo_type or settings.repo_type
        if rt not in REPO_TYPES:
            raise HTTPException(400, f"repo_type 必须是 {sorted(REPO_TYPES)} 之一")
        rev = revision or settings.revision
        if not REVISION_RE.match(rev) or ".." in rev or rev.startswith("/") or rev.endswith("/"):
            raise HTTPException(400, f"revision 非法: {rev}")
        if token:
            if not settings.allow_token_override:
                raise HTTPException(403, "服务端未允许请求级 token 覆盖")
            tok = token
        else:
            tok = settings.token
        if not tok:
            raise HTTPException(503, "服务器未配置 HF token，请设置 HF_TOKEN 或先登录")
        return Target(
            repo_id=rid,
            repo_type=rt,
            revision=rev,
            token=tok,
            private=settings.private if private is None else private,
            auto_create=settings.auto_create_repo,
        )

    async def save_upload(uf: UploadFile, dest: Path, compute_hash: bool) -> tuple[int, Optional[str]]:
        size = 0
        digest = hashlib.sha256() if compute_hash else None
        try:
            async with aiofiles.open(dest, "wb") as fo:
                while True:
                    chunk = await uf.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    size += len(chunk)
                    if settings.max_file_size and size > settings.max_file_size:
                        raise HTTPException(413, f"文件超过上限 {settings.max_file_size} 字节")
                    if digest is not None:
                        digest.update(chunk)
                    await fo.write(chunk)
        finally:
            await uf.close()
        return size, digest.hexdigest() if digest is not None else None

    async def save_stream(request: Request, dest: Path, compute_hash: bool) -> tuple[int, Optional[str]]:
        size = 0
        digest = hashlib.sha256() if compute_hash else None
        async with aiofiles.open(dest, "wb") as fo:
            async for chunk in request.stream():
                if not chunk:
                    continue
                size += len(chunk)
                if settings.max_file_size and size > settings.max_file_size:
                    raise HTTPException(413, f"文件超过上限 {settings.max_file_size} 字节")
                if digest is not None:
                    digest.update(chunk)
                await fo.write(chunk)
        return size, digest.hexdigest() if digest is not None else None

    def finalize_job(job: Job, wait: bool):
        status_code = 200 if wait else 202
        return JSONResponse(status_code=status_code, content=job.public_dict())

    # ------------------------------------------------------------------ routes

    @app.get("/")
    async def root():
        return {
            "service": "hf-upload-server",
            "docs": "/docs",
            "endpoints": [
                "POST /upload/file",
                "POST /upload/url",
                "POST /upload/stream",
                "GET /jobs",
                "GET /jobs/{id}",
                "GET /jobs/{id}/events",
                "GET /queue",
                "GET /metrics",
                "GET /files",
                "GET /whoami",
                "GET /config",
                "POST /repos",
                "GET /dashboard",
                "GET /errors",
            ],
        }

    @app.get("/health")
    async def health():
        return {"status": "ok", "time": now_iso()}

    @app.get("/dashboard", response_class=HTMLResponse)
    async def dashboard():
        return DASHBOARD_HTML

    @app.get("/errors")
    async def error_catalog():
        return {
            "http_status": {
                "400": "参数非法：目录穿越、非法 repo_id、URL 非 http(s) 或指向内网",
                "401": "API Key 缺失或错误",
                "403": "主机不在 allowed_hosts，或请求级 token 覆盖被禁用",
                "404": "任务不存在",
                "413": "文件超过 max_file_size",
                "422": "请求字段校验失败，详见响应 detail",
                "503": "服务器未配置 HF token",
            },
            "job_status": {
                "queued": "已入队，等待执行",
                "running": "执行中（下载 / 加密 / 上传）",
                "success": "全部文件上传成功",
                "partial": "部分文件成功，失败原因见 errors 与 files[].error",
                "failed": "全部文件失败或 commit 失败",
                "cancelled": "服务关闭时任务被取消",
            },
            "file_status": {
                "pending": "等待下载",
                "downloading": "正在下载远程 URL",
                "saved": "本地文件已接收",
                "downloaded": "远程文件已下载",
                "encrypting": "正在 AES 加密",
                "prepared": "已准备好上传",
                "uploading": "正在提交 commit",
                "success": "已上传",
                "failed": "失败，见 error",
                "cancelled": "已取消",
            },
            "common_hf_errors": {
                "401": "HF token 无效",
                "403": "token 无 write 权限或仓库权限不足",
                "404": "仓库不存在且未开启 auto_create_repo",
                "429": "HF commit 频率限制（128 次/小时）：把多个文件放进同一任务以减少 commit，等待额度重置或升级账号",
                "5xx": "HF 服务端错误，服务会自动重试 3 次",
            },
        }

    @app.get("/config")
    async def get_config(_: None = Depends(require_auth)):
        return settings.public_dict()

    @app.get("/whoami")
    async def whoami(_: None = Depends(require_auth)):
        if not settings.token:
            raise HTTPException(503, "服务器未配置 HF token")
        info = await asyncio.to_thread(HfApi(token=settings.token).whoami)
        return {
            "name": info.get("name"),
            "type": info.get("type", "user"),
            "orgs": [o.get("name") for o in info.get("orgs", [])],
        }

    @app.post("/upload/file", status_code=202)
    async def upload_file_endpoint(
        request: Request,
        files: list[UploadFile] = File(..., description="一个或多个文件"),
        dir: str = Form(""),
        repo_id: Optional[str] = Form(None),
        repo_type: Optional[str] = Form(None),
        revision: Optional[str] = Form(None),
        private: Optional[bool] = Form(None),
        encrypt: bool = Form(False),
        password: Optional[str] = Form(None),
        commit_message: Optional[str] = Form(None),
        wait: bool = Form(False),
        token: Optional[str] = Form(None),
        _: None = Depends(require_auth),
    ):
        target = resolve_target(repo_id, repo_type, revision, private, token)
        bucket = safe_dir(dir or settings.default_dir)
        manager = manager_of(request)
        temp_dir = manager.new_temp_dir()
        submitted = False
        try:
            jobs: list[tuple[UploadFile, str, Path]] = []
            used: set[str] = set()
            for uf in files:
                name = unique_name(safe_filename(uf.filename), used)
                jobs.append((uf, name, temp_dir / f"{uuid.uuid4().hex}.part"))
            results = await asyncio.gather(
                *(save_upload(uf, dest, not encrypt) for uf, _, dest in jobs),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, BaseException):
                    raise result
            file_tasks: list[FileTask] = []
            for (uf, name, dest), (size, sha) in zip(jobs, results):
                path_in_repo = posixpath.join(bucket, f"{name}.aes" if encrypt else name)
                file_tasks.append(
                    FileTask(
                        source=name,
                        kind="local",
                        filename=name,
                        path_in_repo=path_in_repo,
                        temp_path=str(dest),
                        size=size,
                        sha256=sha,
                        status="saved",
                    )
                )
            job = manager.create_job(
                target,
                encrypt,
                (password or settings.encrypt_default_password) if encrypt else None,
                commit_message,
                file_tasks,
                temp_dir,
            )
            await manager.submit(job)
            submitted = True
            if wait:
                job = await manager.wait(job.id)
            return finalize_job(job, wait)
        except BaseException:
            if not submitted:
                shutil.rmtree(temp_dir, ignore_errors=True)
            raise

    @app.post("/upload/url", status_code=202)
    async def upload_url_endpoint(
        payload: UrlUploadRequest,
        request: Request,
        _: None = Depends(require_auth),
    ):
        target = resolve_target(payload.repo_id, payload.repo_type, payload.revision, payload.private, payload.token)
        bucket = safe_dir(payload.dir or settings.default_dir)
        await asyncio.gather(*(validate_url(url, settings) for url in payload.urls))
        manager = manager_of(request)
        temp_dir = manager.new_temp_dir()
        submitted = False
        try:
            used: set[str] = set()
            file_tasks: list[FileTask] = []
            for url in payload.urls:
                name = safe_filename(payload.filename) if payload.filename and len(payload.urls) == 1 else safe_filename(filename_from_url(url))
                name = unique_name(name, used)
                path_in_repo = posixpath.join(bucket, f"{name}.aes" if payload.encrypt else name)
                file_tasks.append(
                    FileTask(source=url, kind="url", filename=name, path_in_repo=path_in_repo)
                )
            job = manager.create_job(
                target,
                payload.encrypt,
                (payload.password or settings.encrypt_default_password) if payload.encrypt else None,
                payload.commit_message,
                file_tasks,
                temp_dir,
            )
            await manager.submit(job)
            submitted = True
            if payload.wait:
                job = await manager.wait(job.id)
            return finalize_job(job, payload.wait)
        except BaseException:
            if not submitted:
                shutil.rmtree(temp_dir, ignore_errors=True)
            raise

    @app.post("/upload/stream", status_code=202)
    async def upload_stream_endpoint(
        request: Request,
        filename: str = Query(..., description="目标文件名"),
        dir: str = Query(""),
        repo_id: Optional[str] = Query(None),
        repo_type: Optional[str] = Query(None),
        revision: Optional[str] = Query(None),
        private: Optional[bool] = Query(None),
        encrypt: bool = Query(False),
        password: Optional[str] = Query(None),
        commit_message: Optional[str] = Query(None),
        wait: bool = Query(False),
        token: Optional[str] = Query(None),
        _: None = Depends(require_auth),
    ):
        target = resolve_target(repo_id, repo_type, revision, private, token)
        name = safe_filename(filename)
        bucket = safe_dir(dir or settings.default_dir)
        manager = manager_of(request)
        temp_dir = manager.new_temp_dir()
        dest = temp_dir / f"{uuid.uuid4().hex}.part"
        submitted = False
        try:
            size, sha = await save_stream(request, dest, not encrypt)
            path_in_repo = posixpath.join(bucket, f"{name}.aes" if encrypt else name)
            file_task = FileTask(
                source=name,
                kind="local",
                filename=name,
                path_in_repo=path_in_repo,
                temp_path=str(dest),
                size=size,
                sha256=sha,
                status="saved",
            )
            job = manager.create_job(
                target,
                encrypt,
                (password or settings.encrypt_default_password) if encrypt else None,
                commit_message,
                [file_task],
                temp_dir,
            )
            await manager.submit(job)
            submitted = True
            if wait:
                job = await manager.wait(job.id)
            return finalize_job(job, wait)
        except BaseException:
            if not submitted:
                shutil.rmtree(temp_dir, ignore_errors=True)
            raise

    @app.get("/jobs")
    async def list_jobs(
        request: Request,
        status: Optional[str] = Query(None),
        limit: int = Query(25, ge=1, le=500),
        offset: int = Query(0, ge=0),
        _: None = Depends(require_auth),
    ):
        return await manager_of(request).list_jobs(status, limit, offset)

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str, request: Request, _: None = Depends(require_auth)):
        job = await manager_of(request).get(job_id)
        if job is None:
            raise HTTPException(404, "任务不存在")
        return job.public_dict()

    @app.get("/jobs/{job_id}/events")
    async def job_events(job_id: str, request: Request, _: None = Depends(require_auth)):
        manager = manager_of(request)

        async def event_stream():
            last: Optional[str] = None
            last_emit = time.monotonic()
            deadline = time.monotonic() + 3600
            while True:
                if await request.is_disconnected():
                    break
                job = await manager.get(job_id)
                if job is None:
                    yield 'event: error\ndata: {"error":"job not found"}\n\n'
                    break
                payload = json.dumps(job.public_dict(), ensure_ascii=False)
                now = time.monotonic()
                if payload != last:
                    yield f"data: {payload}\n\n"
                    last = payload
                    last_emit = now
                elif now - last_emit >= 15:
                    yield ": keepalive\n\n"
                    last_emit = now
                if job.status in TERMINAL or now > deadline:
                    break
                await asyncio.sleep(settings.sse_interval)

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/queue")
    async def queue(request: Request, _: None = Depends(require_auth)):
        return await manager_of(request).queue_snapshot()

    @app.get("/metrics")
    async def metrics(request: Request, _: None = Depends(require_auth)):
        return PlainTextResponse(await manager_of(request).metrics_text())

    @app.get("/files")
    async def list_files(
        repo_id: Optional[str] = Query(None),
        repo_type: Optional[str] = Query(None),
        revision: Optional[str] = Query(None),
        token: Optional[str] = Query(None),
        _: None = Depends(require_auth),
    ):
        target = resolve_target(repo_id, repo_type, revision, None, token)
        files = await asyncio.to_thread(
            HfApi(token=target.token).list_repo_files,
            target.repo_id,
            repo_type=target.repo_type,
            revision=target.revision,
        )
        return {"repo_id": target.repo_id, "repo_type": target.repo_type, "revision": target.revision, "count": len(files), "files": files}

    @app.post("/repos", status_code=201)
    async def create_repo(payload: CreateRepoRequest, _: None = Depends(require_auth)):
        if not REPO_ID_RE.match(payload.repo_id):
            raise HTTPException(400, f"repo_id 格式非法: {payload.repo_id}")
        if payload.repo_type not in REPO_TYPES:
            raise HTTPException(400, f"repo_type 必须是 {sorted(REPO_TYPES)} 之一")
        if payload.token:
            if not settings.allow_token_override:
                raise HTTPException(403, "服务端未允许请求级 token 覆盖")
            token = payload.token
        else:
            token = settings.token
        if not token:
            raise HTTPException(503, "服务器未配置 HF token")
        api = HfApi(token=token)
        url = await asyncio.to_thread(
            api.create_repo,
            repo_id=payload.repo_id,
            repo_type=payload.repo_type,
            private=payload.private,
            exist_ok=True,
        )
        return {"repo_id": payload.repo_id, "repo_type": payload.repo_type, "private": payload.private, "url": str(url)}

    return app


# ---------------------------------------------------------------------------
# 启动入口
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hugging Face 异步上传服务")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="配置文件路径（默认 server_config.json）")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--repo-id")
    parser.add_argument("--repo-type", choices=sorted(REPO_TYPES))
    parser.add_argument("--revision")
    parser.add_argument("--token")
    parser.add_argument("--default-dir")
    parser.add_argument("--api-key")
    parser.add_argument("--log-level", default="info")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    overrides = {
        "repo_id": args.repo_id,
        "repo_type": args.repo_type,
        "revision": args.revision,
        "token": args.token,
        "default_dir": args.default_dir,
        "api_key": args.api_key,
    }
    settings = Settings.load(Path(args.config), overrides)
    app = create_app(settings)
    logger.info("starting server on %s:%s (uploads=%s downloads=%s)", args.host, args.port, settings.max_concurrent_uploads, settings.max_concurrent_downloads)
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        workers=1,  # 内存任务队列，必须单 worker
        timeout_keep_alive=75,
        backlog=2048,
    )


app = create_app(Settings.load(DEFAULT_CONFIG_PATH, {}))


if __name__ == "__main__":
    main()
