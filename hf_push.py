#!/usr/bin/env python3
"""上传本地文件/HTTP 文件到 Hugging Face（走 HfApi，无需 git commit/push）。

用法示例:
  ./hfpush upload -s ./books.csv -r user/my-dataset -t dataset
  ./hfpush upload -s https://example.com/data.zip -r user/my-dataset -t dataset \
      --encrypt --password 123456 --path-in-repo data/
  ./hfpush upload -s ./some_folder -r user/my-dataset -t dataset --encrypt

  ./hfpush decrypt -s books.csv.aes -o books.csv
  ./hfpush whoami
"""

import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlparse

MAGIC = b"HFPAES1\n"
SALT_SIZE = 16
NONCE_SIZE = 12
TAG_SIZE = 16
CHUNK_SIZE = 1024 * 1024
KDF_ITERATIONS = 600_000
DEFAULT_PASSWORD = "123456"
PASSWORD_ENV = "HF_PUSH_AES_PASSWORD"
DEFAULT_IGNORE = [".git", ".git/*"]


def human_size(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024 or unit == "TB":
            return f"{num:.2f} {unit}" if unit != "B" else f"{int(num)} B"
        num /= 1024
    return f"{num:.2f} TB"


def is_url(value: str) -> bool:
    return value.lower().startswith(("http://", "https://"))


def get_password(args) -> str:
    if getattr(args, "password", None):
        return args.password
    env = os.environ.get(PASSWORD_ENV)
    if env:
        return env
    return DEFAULT_PASSWORD


def derive_key(password: str, salt: bytes) -> bytes:
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

    return PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=KDF_ITERATIONS,
    ).derive(password.encode("utf-8"))


def ensure_output_ok(path: Path, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise SystemExit(f"输出文件已存在: {path}（加 --overwrite 覆盖）")


def encrypt_file(src: Path, dst: Path, password: str) -> str:
    """AES-256-GCM 流式加密，返回明文 SHA256。

    文件格式: MAGIC | salt(16) | nonce(12) | ciphertext... | tag(16)
    """
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    salt = os.urandom(SALT_SIZE)
    nonce = os.urandom(NONCE_SIZE)
    encryptor = Cipher(algorithms.AES(derive_key(password, salt)), modes.GCM(nonce)).encryptor()
    digest = hashlib.sha256()

    total = src.stat().st_size
    dst.parent.mkdir(parents=True, exist_ok=True)
    progress = _progress_bar(total, f"AES加密 {src.name}")
    try:
        with open(src, "rb") as fi, open(dst, "wb") as fo:
            fo.write(MAGIC)
            fo.write(salt)
            fo.write(nonce)
            while True:
                chunk = fi.read(CHUNK_SIZE)
                if not chunk:
                    break
                digest.update(chunk)
                fo.write(encryptor.update(chunk))
                if progress:
                    progress.update(len(chunk))
            fo.write(encryptor.finalize())
            fo.write(encryptor.tag)
    except BaseException:
        dst.unlink(missing_ok=True)
        raise
    finally:
        if progress:
            progress.close()
    return digest.hexdigest()


def decrypt_file(src: Path, dst: Path, password: str) -> str:
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    dst.parent.mkdir(parents=True, exist_ok=True)
    size = src.stat().st_size
    header = len(MAGIC) + SALT_SIZE + NONCE_SIZE
    if size < header + TAG_SIZE:
        raise SystemExit(f"不是有效的加密文件（文件过小）: {src}")

    with open(src, "rb") as fi:
        if fi.read(len(MAGIC)) != MAGIC:
            raise SystemExit(f"不是本工具生成的 AES 加密文件: {src}")
        salt = fi.read(SALT_SIZE)
        nonce = fi.read(NONCE_SIZE)
        fi.seek(size - TAG_SIZE)
        tag = fi.read(TAG_SIZE)
        ciphertext_size = size - header - TAG_SIZE
        fi.seek(header)

        decryptor = Cipher(algorithms.AES(derive_key(password, salt)), modes.GCM(nonce)).decryptor()
        digest = hashlib.sha256()
        progress = _progress_bar(ciphertext_size, f"AES解密 {src.name}")
        try:
            with open(dst, "wb") as fo:
                remaining = ciphertext_size
                while remaining > 0:
                    chunk = fi.read(min(CHUNK_SIZE, remaining))
                    if not chunk:
                        raise SystemExit("加密文件被截断")
                    remaining -= len(chunk)
                    plain = decryptor.update(chunk)
                    digest.update(plain)
                    fo.write(plain)
                    if progress:
                        progress.update(len(chunk))
                try:
                    tail = decryptor.finalize_with_tag(tag)
                    digest.update(tail)
                    fo.write(tail)
                except InvalidTag:
                    raise SystemExit("解密失败：密码错误或文件已被损坏")
        except BaseException:
            dst.unlink(missing_ok=True)
            raise
        finally:
            if progress:
                progress.close()
    return digest.hexdigest()


def _progress_bar(total: int, desc: str):
    if not sys.stderr.isatty() or total <= 0:
        return None
    from tqdm import tqdm

    return tqdm(total=total, desc=desc, unit="B", unit_scale=True, unit_divisor=1024, leave=False)


def download_file(url: str, dest_dir: Path, overwrite: bool = False) -> Path:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.headers.update({"User-Agent": "hf-push/1.0"})
    dest_dir.mkdir(parents=True, exist_ok=True)

    with session.get(url, stream=True, timeout=(15, 120)) as resp:
        resp.raise_for_status()
        name = filename_from_response(url, resp)
        target = dest_dir / name
        if target.exists() and not overwrite:
            stem, suffix = target.stem, target.suffix
            i = 1
            while target.exists():
                target = dest_dir / f"{stem}-{i}{suffix}"
                i += 1
        total = int(resp.headers.get("Content-Length") or 0)
        progress = _progress_bar(total, f"下载 {name}")
        try:
            with open(target, "wb") as fo:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        fo.write(chunk)
                        if progress:
                            progress.update(len(chunk))
        except BaseException:
            target.unlink(missing_ok=True)
            raise
        finally:
            if progress:
                progress.close()
    return target


def filename_from_response(url: str, resp) -> str:
    cd = resp.headers.get("Content-Disposition", "")
    match = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", cd, re.IGNORECASE)
    if not match:
        match = re.search(r'filename\s*=\s*"?([^";]+)"?', cd, re.IGNORECASE)
    if match:
        name = unquote(match.group(1)).strip().strip('"')
    else:
        name = unquote(os.path.basename(urlparse(url).path))
    name = Path(name).name
    return name or "download.bin"


def resolve_token(args) -> str:
    from huggingface_hub import get_token

    token = (
        getattr(args, "token", None)
        or os.environ.get("HF_TOKEN")
        or os.environ.get("HUGGING_FACE_HUB_TOKEN")
        or get_token()
    )
    if not token:
        raise SystemExit(
            "未找到 Hugging Face token。请先登录：\n"
            "  .venv/bin/hf auth login\n"
            "或设置环境变量 HF_TOKEN（需要 write 权限）。"
        )
    return token


def repo_url(repo_id: str, repo_type: str) -> str:
    from huggingface_hub import constants

    endpoint = constants.ENDPOINT.rstrip("/")
    if repo_type == "dataset":
        return f"{endpoint}/datasets/{repo_id}"
    if repo_type == "space":
        return f"{endpoint}/spaces/{repo_id}"
    return f"{endpoint}/{repo_id}"


def target_for_file(args, name: str, multiple: bool) -> str:
    """计算单个文件在仓库中的目标路径。"""
    prefix = (args.path_in_repo or "").strip("/")
    if multiple:
        return f"{prefix}/{name}" if prefix else name
    if args.path_in_repo is None:
        return name
    if args.path_in_repo.endswith("/") or prefix == "":
        return f"{prefix}/{name}" if prefix else name
    return prefix


def ignored(path: Path, root: Path) -> bool:
    return ".git" in path.relative_to(root).parts


def iter_folder_files(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d != ".git")
        for filename in sorted(filenames):
            path = Path(dirpath) / filename
            if ignored(path, root):
                continue
            yield path


class TempDir:
    def __init__(self, keep: bool):
        self.keep = keep
        self.path = Path(tempfile.mkdtemp(prefix="hfpush-"))

    def __enter__(self) -> Path:
        return self.path

    def __exit__(self, exc_type, exc, tb):
        if self.keep:
            print(f"临时文件已保留: {self.path}", file=sys.stderr)
        elif self.path.exists():
            shutil.rmtree(self.path, ignore_errors=True)


def upload_one_file(api, args, token, local: Path, target: str, password: str):
    commit_message = args.commit_message or f"Upload {target}"
    if args.dry_run:
        print(f"[dry-run] 上传 {local}  ->  {target}")
        return
    info = api.upload_file(
        path_or_fileobj=str(local),
        path_in_repo=target,
        repo_id=args.repo_id,
        repo_type=args.repo_type,
        token=token,
        revision=args.revision,
        commit_message=commit_message,
    )
    print(f"已上传: {target}" + (f"  (commit {info.oid[:8]})" if getattr(info, "oid", None) else ""))


def cmd_upload(args) -> None:
    from huggingface_hub import HfApi

    token = None if args.dry_run else resolve_token(args)
    api = HfApi(token=token)

    if not args.dry_run:
        try:
            api.create_repo(
                repo_id=args.repo_id,
                repo_type=args.repo_type,
                private=True if args.private else None,
                exist_ok=True,
                token=token,
            )
        except Exception as exc:
            raise SystemExit(f"创建/访问仓库失败: {exc}")

    sources = args.source
    multiple = len(sources) > 1
    password = get_password(args)
    used_password_default = args.encrypt and not (args.password or os.environ.get(PASSWORD_ENV))
    if used_password_default:
        print(f"提示: 未指定密码，使用默认密码 {DEFAULT_PASSWORD!r}", file=sys.stderr)

    targets_seen = {}
    with TempDir(args.keep_temp) as tmp:
        for source in sources:
            if is_url(source):
                if args.dry_run:
                    name = Path(unquote(os.path.basename(urlparse(source).path))).name or "download.bin"
                    if args.encrypt:
                        name = f"{name}.aes"
                    target = target_for_file(args, name, multiple)
                    if target in targets_seen:
                        raise SystemExit(f"目标路径冲突: {target}")
                    targets_seen[target] = source
                    print(f"[dry-run] 下载 {source}  ->  {target}")
                    continue
                print(f"下载: {source}")
                local = download_file(source, tmp / "download", overwrite=True)
            else:
                local = Path(source).expanduser()
                if not local.exists():
                    raise SystemExit(f"源文件不存在: {local}")

            if local.is_dir():
                if args.path_in_repo is not None and not args.path_in_repo.endswith("/") and multiple:
                    raise SystemExit("多个源或目录上传时 --path-in-repo 应作为目录前缀")
                if args.encrypt:
                    mirror = tmp / "encrypted" / local.name
                    print(f"AES加密目录: {local} -> {mirror}")
                    for path in iter_folder_files(local):
                        rel = path.relative_to(local)
                        dst = mirror / f"{rel}.aes"
                        print(f"  加密 {rel} ({human_size(path.stat().st_size)})")
                        if not args.dry_run:
                            sha = encrypt_file(path, dst, password)
                            print(f"  明文 SHA256: {sha}")
                    if args.dry_run:
                        print(f"[dry-run] 上传目录 {mirror}  ->  {args.path_in_repo or '/'}")
                    else:
                        api.upload_folder(
                            folder_path=str(mirror),
                            path_in_repo=(args.path_in_repo or None),
                            repo_id=args.repo_id,
                            repo_type=args.repo_type,
                            token=token,
                            revision=args.revision,
                            commit_message=args.commit_message or f"Upload {local.name} (AES encrypted)",
                        )
                        print(f"已上传目录: {local.name} -> {args.path_in_repo or '/'}")
                else:
                    if args.dry_run:
                        print(f"[dry-run] 上传目录 {local}  ->  {args.path_in_repo or '/'}")
                    else:
                        api.upload_folder(
                            folder_path=str(local),
                            path_in_repo=(args.path_in_repo or None),
                            repo_id=args.repo_id,
                            repo_type=args.repo_type,
                            token=token,
                            revision=args.revision,
                            ignore_patterns=DEFAULT_IGNORE,
                            commit_message=args.commit_message or f"Upload {local.name}",
                        )
                        print(f"已上传目录: {local.name} -> {args.path_in_repo or '/'}")
                continue

            upload_local = local
            name = local.name
            if args.encrypt:
                name = f"{name}.aes"
                encrypted = tmp / "encrypted" / name
                print(f"AES加密: {local} ({human_size(local.stat().st_size)})")
                if not args.dry_run:
                    sha = encrypt_file(local, encrypted, password)
                    print(f"明文 SHA256: {sha}")
                upload_local = encrypted

            target = target_for_file(args, name, multiple)
            if target in targets_seen:
                raise SystemExit(f"目标路径冲突: {target}（来源 {targets_seen[target]} 与 {local}）")
            targets_seen[target] = str(local)
            upload_one_file(api, args, token, upload_local, target, password)

    print(f"完成。仓库地址: {repo_url(args.repo_id, args.repo_type)}")


def cmd_encrypt(args) -> None:
    src = Path(args.source).expanduser()
    if not src.is_file():
        raise SystemExit(f"源文件不存在: {src}")
    dst = Path(args.output).expanduser() if args.output else src.with_name(src.name + ".aes")
    ensure_output_ok(dst, args.overwrite)
    password = get_password(args)
    if not (args.password or os.environ.get(PASSWORD_ENV)):
        print(f"提示: 未指定密码，使用默认密码 {DEFAULT_PASSWORD!r}", file=sys.stderr)
    sha = encrypt_file(src, dst, password)
    print(f"已加密: {src} -> {dst}")
    print(f"明文 SHA256: {sha}")


def cmd_decrypt(args) -> None:
    src = Path(args.source).expanduser()
    if not src.is_file():
        raise SystemExit(f"源文件不存在: {src}")
    if args.output:
        dst = Path(args.output).expanduser()
    else:
        dst = src.with_suffix("") if src.suffix == ".aes" else src.with_name(src.name + ".dec")
    ensure_output_ok(dst, args.overwrite)
    password = get_password(args)
    sha = decrypt_file(src, dst, password)
    print(f"已解密: {src} -> {dst}")
    print(f"明文 SHA256: {sha}")


def cmd_whoami(args) -> None:
    from huggingface_hub import HfApi

    token = resolve_token(args)
    info = HfApi(token=token).whoami()
    print(f"已登录: {info.get('name')} ({info.get('type', 'user')})")
    orgs = [o.get("name") for o in info.get("orgs", [])]
    if orgs:
        print("所属组织: " + ", ".join(filter(None, orgs)))


def cmd_login(args) -> None:
    hf_bin = Path(sys.executable).with_name("hf")
    if not hf_bin.exists():
        found = shutil.which("hf")
        if not found:
            raise SystemExit("未找到 hf CLI，请手动运行 .venv/bin/hf auth login")
        hf_bin = Path(found)
    cmd = [str(hf_bin), "auth", "login"]
    if args.token:
        cmd += ["--token", args.token]
    if args.force:
        cmd += ["--force"]
    code = subprocess.run(cmd).returncode
    if code != 0:
        raise SystemExit(f"登录失败（退出码 {code}）")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hf_push",
        description="通过 Hugging Face API 直接上传文件（无需 git commit/push），支持本地或 HTTP(S) 源与可选 AES 加密。",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_password(p):
        p.add_argument("--password", help=f"AES 密码（默认取环境变量 {PASSWORD_ENV}，否则 {DEFAULT_PASSWORD}）")

    p_up = sub.add_parser("upload", help="上传文件/目录到 Hugging Face")
    p_up.add_argument("-s", "--source", nargs="+", required=True, help="本地路径或 http(s) URL，可多个")
    p_up.add_argument("-r", "--repo-id", required=True, help="仓库 ID，如 user/name")
    p_up.add_argument("-t", "--repo-type", default="dataset", choices=["dataset", "model", "space"], help="仓库类型（默认 dataset）")
    p_up.add_argument("--path-in-repo", help="仓库内目标路径；单文件可写完整路径，目录/多文件作为目录前缀")
    p_up.add_argument("-e", "--encrypt", action="store_true", help="上传前进行 AES-256-GCM 加密")
    add_password(p_up)
    p_up.add_argument("--private", action="store_true", help="创建私有仓库（已存在的仓库保持原可见性）")
    p_up.add_argument("--token", help="HF token（默认 HF_TOKEN 或 hf auth login 的缓存）")
    p_up.add_argument("--revision", default="main", help="目标分支/revision（默认 main）")
    p_up.add_argument("--commit-message", help="提交信息")
    p_up.add_argument("--keep-temp", action="store_true", help="保留下载/加密的临时文件")
    p_up.add_argument("--dry-run", action="store_true", help="只打印计划，不下载/加密/上传")
    p_up.set_defaults(func=cmd_upload)

    p_enc = sub.add_parser("encrypt", help="仅本地 AES 加密")
    p_enc.add_argument("-s", "--source", required=True)
    p_enc.add_argument("-o", "--output")
    add_password(p_enc)
    p_enc.add_argument("--overwrite", action="store_true")
    p_enc.set_defaults(func=cmd_encrypt)

    p_dec = sub.add_parser("decrypt", help="解密本工具生成的 .aes 文件")
    p_dec.add_argument("-s", "--source", required=True)
    p_dec.add_argument("-o", "--output")
    add_password(p_dec)
    p_dec.add_argument("--overwrite", action="store_true")
    p_dec.set_defaults(func=cmd_decrypt)

    p_who = sub.add_parser("whoami", help="查看当前 HF 登录身份")
    p_who.add_argument("--token")
    p_who.set_defaults(func=cmd_whoami)

    p_login = sub.add_parser("login", help="登录 Hugging Face（浏览器或 token）")
    p_login.add_argument("--token", help="直接提供 access token（需 write 权限）")
    p_login.add_argument("--force", action="store_true", help="强制重新登录")
    p_login.set_defaults(func=cmd_login)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\n已取消", file=sys.stderr)
        return 130
    except SystemExit:
        raise
    except Exception as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
