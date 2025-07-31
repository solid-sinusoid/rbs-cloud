"""
web_client.py — Streamlit‑клиент без зависимостей от pandas/numpy.
Работает с вашим FastAPI‑сервером: загружает файлы по одному, показывает
/preview и /list простыми таблицами.
"""

from io import BytesIO
from pathlib import Path

import requests
import streamlit as st
from rbs_client.upload_dataset import upload_dataset_to_server
from rbs_client.web_utils import (
    _safe_rerun,
    upload_directory,
    fetch_preview,
)

API_URL = "http://msi.lan:8000"  # меняйте при необходимости

# -----------------------------------------------------------------------------
# Утилиты
# -----------------------------------------------------------------------------





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
        dataset_name = st.text_input(
            "Имя датасета на сервере", placeholder="dataset_name"
        )
        if st.button("Загрузить датасет на сервер"):
            if not dir_path_str or not Path(dir_path_str).is_dir():
                st.warning("Путь не существует или не директория")
            elif not dataset_name:
                st.warning("Укажите имя датасета")
            else:
                with st.spinner("Загрузка датасета…"):
                    upload_dataset_to_server(dataset_name, dir_path_str, API_URL)
                st.success(f"Датасет '{dataset_name}' загружен")
                _safe_rerun()

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
