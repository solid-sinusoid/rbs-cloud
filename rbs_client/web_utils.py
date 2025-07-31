from io import BytesIO
from pathlib import Path

import requests
import streamlit as st

API_URL = "http://msi.lan:8000"  # Default, can be overridden


def _safe_rerun():
    """Rerun Streamlit app if possible."""
    if hasattr(st, "experimental_rerun"):
        st.experimental_rerun()
    elif hasattr(st, "rerun"):
        st.rerun()


def upload_file(path: Path, api_url: str = API_URL):
    """Upload a single file to the server."""
    with path.open("rb") as fh:
        files = {"file": (path.name, fh, "application/octet-stream")}
        requests.post(f"{api_url}/upload", files=files).raise_for_status()


def upload_directory(dir_path: Path, api_url: str = API_URL):
    """Upload all files inside *dir_path* recursively."""
    file_list = [p for p in dir_path.rglob("*") if p.is_file()]
    total = len(file_list)
    if total == 0:
        st.warning("Папка пуста")
        return 0, 0

    prog = st.progress(0.0, text="Загрузка файлов…")
    ok = err = 0
    for idx, file in enumerate(file_list, 1):
        try:
            upload_file(file, api_url)
            ok += 1
        except Exception as exc:
            st.warning(f"{file.name}: {exc}")
            err += 1
        prog.progress(idx / total, text=f"{idx}/{total} загружено…")
    prog.empty()
    return ok, err


def fetch_preview(file: str, limit: int, api_url: str = API_URL):
    """Return /preview data as a list of dicts or None on error."""
    try:
        resp = requests.get(f"{api_url}/preview", params={"file": file, "limit": limit})
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        st.error(f"Ошибка /preview {file}: {exc}")
        return None


def list_uploaded(api_url: str = API_URL):
    """Return a list of uploaded files from the server."""
    try:
        resp = requests.get(f"{api_url}/list", params={"name": ""})
        resp.raise_for_status()
        return resp.json().get("files", [])
    except Exception as exc:
        st.error(f"Ошибка /list: {exc}")
        return []

