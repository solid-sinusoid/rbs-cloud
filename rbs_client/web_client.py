"""
web_client.py — Streamlit‑клиент без зависимостей от pandas/numpy.
Работает с вашим FastAPI‑сервером: загружает файлы по одному, показывает
/preview и /list простыми таблицами.
"""

from io import BytesIO
from pathlib import Path
import json

from fastapi.openapi.models import APIKey
import requests
import streamlit as st

API_URL = "http://msi.lan:8000"  # меняйте при необходимости

# -----------------------------------------------------------------------------
# Утилиты
# -----------------------------------------------------------------------------


def _safe_rerun():
    if hasattr(st, "experimental_rerun"):
        st.experimental_rerun()
    elif hasattr(st, "rerun"):
        st.rerun()


def upload_file(path: Path):
    with path.open("rb") as fh:
        files = {"file": (path.name, fh, "application/octet-stream")}
        requests.post(f"{API_URL}/upload", files=files).raise_for_status()


def upload_directory(dir_path: Path):
    file_list = [p for p in dir_path.rglob("*") if p.is_file()]
    total = len(file_list)
    if total == 0:
        st.warning("Папка пуста")
        return 0, 0

    prog = st.progress(0.0, text="Загрузка файлов…")
    ok = err = 0
    for idx, file in enumerate(file_list, 1):
        try:
            upload_file(file)
            ok += 1
        except Exception as exc:
            st.warning(f"{file.name}: {exc}")
            err += 1
        prog.progress(idx / total, text=f"{idx}/{total} загружено…")
    prog.empty()
    return ok, err


def fetch_preview(file: str, limit: int):
    """Возвращает данные /preview в виде списка dict либо None при ошибке."""
    try:
        resp = requests.get(f"{API_URL}/preview",
                            params={"file": file, "limit": limit})
        resp.raise_for_status()
        return resp.json()          # ← главное изменение
    except Exception as exc:
        st.error(f"Ошибка /preview {file}: {exc}")
        return None


def list_uploaded():
    try:
        resp = requests.get(f"{API_URL}/list", params={"name": ""})
        resp.raise_for_status()
        return resp.json().get("files", [])
    except Exception as exc:
        st.error(f"Ошибка /list: {exc}")
        return []


# -----------------------------------------------------------------------------
# CSS — скрыть лишние элементы
# -----------------------------------------------------------------------------
_HIDE = """
<style>
#MainMenu, header, footer {visibility: hidden;}
div[data-testid="stDeployButton"], .viewerBadge_container__1QSob {display: none;}
</style>
"""

# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------


def main():
    st.set_page_config("RBS Uploader", page_icon="📂", layout="wide")
    st.markdown(_HIDE, unsafe_allow_html=True)

    left, right = st.columns([3, 2], gap="large")

    # ---------------- ЛЕВАЯ КОЛОНКА ----------------
    with left:
        st.header("Загрузка датасета (папка)")
        dir_path_str = st.text_input(
            "Полный путь к папке", placeholder="/abs/path/to/dir"
        )
        if dir_path_str and Path(dir_path_str).is_dir():
            if st.button("Загрузить все файлы"):
                with st.spinner("Отправляем файлы…"):
                    ok, err = upload_directory(Path(dir_path_str))
                st.success(f"Успешно: {ok}, ошибок: {err}")
                _safe_rerun()
        elif dir_path_str:
            st.warning("Путь не существует или не директория")

        st.divider()
        st.header("Или перетащите файлы")
        files = st.file_uploader("Выберите файлы", accept_multiple_files=True)
        if files and st.button("Загрузить выбранные"):
            ok = err = 0
            with st.spinner("Передаём…"):
                for f in files:
                    try:
                        requests.post(
                            f"{API_URL}/upload",
                            files={
                                "file": (
                                    f.name,
                                    BytesIO(f.getvalue()),
                                    "application/octet-stream",
                                )
                            },
                        ).raise_for_status()
                        ok += 1
                    except Exception as exc:
                        st.warning(f"{f.name}: {exc}")
                        err += 1
            st.success(f"Успешно: {ok}, ошибок: {err}")
            _safe_rerun()

    # ---------------- ПРАВАЯ КОЛОНКА ----------------
    with right:
        st.header("Просмотр данных в rbs-cloud")

        st.subheader("Список датасетов")
        preview_ds = fetch_preview("datasets", 10)
        if preview_ds:
            st.dataframe(preview_ds, use_container_width=True)
        else:
            st.info("Список датасетов пуст")

        st.subheader("Список весов")
        preview_w = fetch_preview("weights", 10)
        if preview_w:
            st.dataframe(preview_w, use_container_width=True)
        else:
            st.info("Список весов пуст")

        st.button("Обновить", on_click=_safe_rerun)


if __name__ == "__main__":
    main()
