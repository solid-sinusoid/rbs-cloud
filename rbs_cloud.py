import os
import shutil
import sys
import subprocess
import shlex
import datetime
import threading
import time
import errno
import signal
from typing import Optional
from enum import Enum
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel
from fastapi import FastAPI, Query, HTTPException, File, UploadFile, Form
from fastapi.responses import FileResponse, JSONResponse

app = FastAPI()

# Папка с файлами БД - .parquet
DIR_DATA = "data"
# Папка кэша
DIR_CACHE = "cache"
# Пути к parquet-файлам
DATASET_FILE = DIR_DATA + "/datasets.parquet"
WEIGHTS_FILE = DIR_DATA + "/weights.parquet"

class DatasetStatus(str, Enum):
    CREATING = "creating"
    SAVE = "save"
    CONVERSION = "conversion"
    STORE = "store"
    AT_WORK = "at work" # is weights

if not os.path.exists(DATASET_FILE):
    # Опишем схему
    schema = pa.schema([
        # ("id", pa.int32()),
        ("name", pa.string()),
        ("num_episodes", pa.int32()),
        ("src_format", pa.string()),
        ("work_format", pa.string()),
        ("status", pa.string())
    ])
    # Создаем пустую таблицу с заданной схемой
    table = pa.Table.from_pandas(pd.DataFrame(columns=schema.names), schema=schema)
    # Сохраняем таблицу в файл .parquet
    pq.write_table(table, DATASET_FILE)

if not os.path.exists(WEIGHTS_FILE):
    schema = pa.schema([
        # ("id", pa.int32()),
        ("name", pa.string()),
        ("dataset", pa.string()),
        ("steps", pa.int32())
    ])
    table = pa.Table.from_pandas(pd.DataFrame(columns=schema.names), schema=schema)
    pq.write_table(table, WEIGHTS_FILE)

# Простой helper для проверки, жив ли процесс (работает на Unix/Windows)
def is_process_alive(pid: int) -> bool:
    try:
        if pid <= 0:
            return False
        # На Unix: посылаем 0 сигнал
        os.kill(pid, 0)
    except OSError as e:
        if e.errno in (errno.ESRCH,):  # No such process
            return False
        if e.errno in (errno.EPERM,):  # Нет прав, но процесс есть
            return True
        return False
    else:
        return True

def monitor_conversion(dataset_name: str, pid: int, log_file: Path, pid_file: Path, process: subprocess.Popen): #, dataset_mask_func):
    """Ждёт завершения процесса и обновляет статус."""
    try:
        retcode = process.wait()
        # Читаем текущий df, обновляем статус в зависимости от результата
        try:
            df = pd.read_parquet(DATASET_FILE)
            mask = df["name"] == dataset_name #dataset_mask_func(df)
            if not mask.any():
                # Не нашли — странно, оставляем как есть
                return
            if retcode == 0:
                df.loc[mask, "status"] = DatasetStatus.STORE
            else:
                df.loc[mask, "status"] = DatasetStatus.SAVE  # откатываем
                with open(log_file, "ab") as lf:
                    lf.write(f"\n[!] Conversion exited with code {retcode}\n".encode("utf-8"))
            # Пишем обратно
            df.to_parquet(DATASET_FILE, index=False, engine="pyarrow")
        except Exception as e:
            with open(log_file, "ab") as lf:
                lf.write(f"\n[!] Failed to update status after conversion: {e}\n".encode("utf-8"))
    finally:
        # Удаляем pid-файл, потому что конвертация завершена
        try:
            if pid_file.exists():
                pid_file.unlink()
        except Exception:
            pass

def get_dataset_info(name: str) -> dict:
    df = duckdb.query(f"SELECT * FROM '{DATASET_FILE}' WHERE name = '{name}'").to_df()
    return df.to_dict(orient="records")

@app.get("/")
def root():
    return {"message": "Rbs Cloud (DuckDB Parquet API) работает!"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/create-dataset/")
async def create_dataset(dataset_name: str):
    # Проверим на повтор
    ds_info = get_dataset_info(dataset_name)
    if ds_info:
        raise HTTPException(status_code=500, detail=f"Repeat name '{dataset_name}'")

    # Открываем соединение с DuckDB
    con = duckdb.connect()

    try:
        # Начинаем транзакцию
        con.execute("BEGIN;")

        new_d = {
            # "id": cid,
            "name": dataset_name,
            "num_episodes": 0,
            "src_format": "rosbag",
            "work_format": "lerobot",
            "status": DatasetStatus.CREATING
        }
        # Добавляем новую запись в DATASET_FILE
        new_df = pd.DataFrame([new_d])

        # Читаем существующий файл, если он существует
        try:
            existing_df = pd.read_parquet(DATASET_FILE)
            # Объединяем существующий DataFrame с новым
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        except FileNotFoundError:
            # Если файл не существует, просто используем новый DataFrame
            combined_df = new_df

        # Сохраняем объединенный DataFrame обратно в файл
        combined_df.to_parquet(DATASET_FILE, index=False, engine='pyarrow')

        # Завершаем транзакцию
        con.execute("COMMIT;")
        # !!! Будет создана при конвертации в lerobot
        # # Создадим папку датасета
        # ds_path.mkdir()
    except Exception as e:
        # В случае ошибки откатываем транзакцию
        con.execute("ROLLBACK;")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()

    return {"message": f"Dataset '{dataset_name}' added successfully"}

def conversion_dataset(dataset_name:str):
    # Дедупликация: если уже есть PID и процесс жив — отклоняем
    log_dir = Path(DIR_CACHE)
    log_dir.mkdir(parents=True, exist_ok=True)
    pid_file = log_dir / f"convert_{dataset_name}.pid"

    if pid_file.exists():
        try:
            existing_pid = int(pid_file.read_text().strip())
            if is_process_alive(existing_pid):
                raise HTTPException(
                    status_code=409,
                    detail=f"Conversion already in progress for dataset '{dataset_name}' (pid={existing_pid})"
                )
            else:
                # Старая запись мёртвая — очищаем
                pid_file.unlink()
        except ValueError:
            # битый файл — удалим
            try:
                pid_file.unlink()
            except Exception:
                pass

    # Установим статус на CONVERSION
    try:
        df = pd.read_parquet(DATASET_FILE)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    mask = df["name"] == dataset_name
    if not mask.any():
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' metadata not found")

    try:
        df.loc[mask, "status"] = DatasetStatus.CONVERSION
        df.to_parquet(DATASET_FILE, index=False, engine="pyarrow")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update status metadata: {e}")

    # Подготовка логов и запуск
    # timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_file = log_dir / f"convert_{dataset_name}_{timestamp}.log"

    script_path = Path("convert_rosbag_to_lerobot.py").resolve()
    if not script_path.exists():
        # откатываем статус
        try:
            df = pd.read_parquet(DATASET_FILE)
            df.loc[df["name"] == dataset_name, "status"] = DatasetStatus.SAVE
            df.to_parquet(DATASET_FILE, index=False, engine="pyarrow")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Conversion script not found at {script_path}")

    # Формируем аргументы
    json_out = (Path(DIR_CACHE) / f"{dataset_name}_msg.json").resolve()
    images_out = (Path(DIR_CACHE) / f"{dataset_name}_frames").resolve()
    cmd = [
        sys.executable,
        str(script_path),
        str(Path(DIR_CACHE) / dataset_name),
        "--output", str(Path(DIR_DATA) / dataset_name),
        "--json", str(json_out),
        "--images", str(images_out),
    ]

    try:
        with open(log_file, "wb") as lf:
            process = subprocess.Popen(
                cmd,
                stdout=lf,
                stderr=subprocess.STDOUT,
                start_new_session=True
            )
        # Записываем PID
        pid_file.write_text(str(process.pid))

        # # Запускаем мониторящий поток
        # def dataset_mask_func(df_inner):
        #     return df_inner["name"] == dataset_name

        monitor_thread = threading.Thread(
            target=monitor_conversion,
            args=(dataset_name, process.pid, log_file, pid_file, process), # dataset_mask_func),
            daemon=True,
            name=f"monitor_conv_{dataset_name}"
        )
        monitor_thread.start()

    except Exception as e:
        # Откат статуса
        try:
            df = pd.read_parquet(DATASET_FILE)
            df.loc[df["name"] == dataset_name, "status"] = DatasetStatus.SAVE
            df.to_parquet(DATASET_FILE, index=False, engine="pyarrow")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Failed to start conversion: {e}")

    return {
        "message": f"Dataset '{dataset_name}' conversion started in background",
        "conversion_log": str(log_file),
        "conversion_pid_file": str(pid_file),
        "pid": process.pid,
    }

@app.post("/save-dataset/")
async def save_dataset(dataset_name: str):
    """Finalize dataset creation by updating its status."""
    ds_path = Path(DIR_DATA) / dataset_name

    # Проверим на наличие
    ds_info = get_dataset_info(dataset_name)
    if not ds_info:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not created")
    ds_status = ds_info[0]["status"]
    if not ds_status == DatasetStatus.CREATING:
        raise HTTPException(status_code=500, detail=f"Датасет '{dataset_name}' имеет статус '{ds_status}'")

    # Обновляем статус датасета
    try:
        df = pd.read_parquet(DATASET_FILE)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    mask = df["name"] == dataset_name
    if not mask.any():
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' metadata not found")

    try:
        df.loc[mask, "status"] = DatasetStatus.SAVE
        df.to_parquet(DATASET_FILE, index=False, engine="pyarrow")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update status metadata: {e}")

    return conversion_dataset(dataset_name)

@app.post("/convert-dataset/")
async def convert_dataset(dataset_name: str):
    # Проверим на наличие
    ds_info = get_dataset_info(dataset_name)
    if not ds_info:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not created")
    ds_status = ds_info[0]["status"]
    if not ds_status == DatasetStatus.SAVE:
        raise HTTPException(status_code=500, detail=f"Dataset '{dataset_name}' is '{ds_status}'")

    return conversion_dataset(dataset_name)

def safe_relative_path(rel_path: str) -> Path:
    """
    Проверка и нормализация относительного пути: запрещаем выход за пределы через '..'
    """
    p = Path(rel_path)
    if any(part == ".." for part in p.parts):
        raise ValueError("Относительный путь содержит запрещённые сегменты '..'")
    return p

@app.post("/upload-rel")
async def upload_rel(
    dataset_name: str = Form(...),
    relative_path: str = Form(...),
    file: UploadFile = File(...)
):
    ds_info = get_dataset_info(dataset_name)
    if not ds_info:
        raise HTTPException(status_code=404, detail=f"Датасет '{dataset_name}' не создан")
    if not ds_info[0]["status"] == DatasetStatus.CREATING:
        raise HTTPException(status_code=405, detail=f"Датасет '{dataset_name}' уже сохранён")

    print(f"{ds_info=}")
    try:
        relp = safe_relative_path(relative_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    dataset_path = Path(DIR_CACHE) / dataset_name
    dest_path = dataset_path / relp
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Сохраняем файл
        with open(dest_path, "wb") as f:
            content = await file.read()
            f.write(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка сохранения файла: {e}")

    return JSONResponse({"status": "ok", "saved_to": str(dest_path)})


@app.post("/upload-weights")
async def upload_weights(
    weights_name: str = Form(...),
    relative_path: str = Form(...),
    file: UploadFile = File(...),
):
    """Загрузка весов (папка без архива)."""
    try:
        relp = safe_relative_path(relative_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    weights_root = Path(DIR_DATA) / "weights" / weights_name
    dest_path = weights_root / relp
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(dest_path, "wb") as f:
            content = await file.read()
            f.write(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка сохранения файла: {e}")

    return JSONResponse({"status": "ok", "saved_to": str(dest_path)})

# # dataset весь целиком
# @app.post("/upload-dataset")
# async def upload_dataset(
#     dataset_name: str = Form(...),
#     files: List[UploadFile] = File(...)
# ):
#     dataset_path = Path(DIR_CACHE) / dataset_name

#     try:
#         for file in files:
#             # Восстановление относительного пути
#             relative_path = Path(file.filename)
#             dest_path = dataset_path / relative_path

#             # Создание всех промежуточных директорий
#             dest_path.parent.mkdir(parents=True, exist_ok=True)

#             # Сохранение файла
#             with open(dest_path, "wb") as f:
#                 f.write(await file.read())

#         return {"status": "success", "message": f"Dataset '{dataset_name}' uploaded."}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to upload: {e}")

@app.get("/preview")
def preview(file:str = "file_ids", limit: int = 10):
    table = DIR_DATA + "/" + file + ".parquet"
    try:
        df = duckdb.query(f"SELECT * FROM '{table}' LIMIT {limit}").to_df()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/query")
def run_query(sql: str = Query(..., description="SQL-запрос к parquet-файлу")):
    try:
        if "drop" in sql.lower() or "delete" in sql.lower():
            raise HTTPException(status_code=400, detail="Модифицирующие запросы запрещены.")
        df = duckdb.query(sql).to_df()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

class UploadParams(BaseModel):
    dataset_name: Optional[str] = "new_ds"

@app.post("/upload")
def upload(file: UploadFile = File(...)):
    save_path = os.path.join(DIR_CACHE, file.filename)
    try:
        with open(save_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return {"message": f"Файл '{file.filename}' успешно сохранён в {DIR_CACHE}/."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class ProcessParams(BaseModel):
    episode: Optional[int] = None
    # columns: Optional[List[str]] = None
    mode: Optional[str] = "convert"

@app.post("/process")
def process_uploaded_files(params: ProcessParams):
    # parquet_files = [f for f in os.listdir("data") if f.endswith(".parquet")]
    # if not parquet_files:
    #     raise HTTPException(status_code=404, detail="Нет загруженных файлов для обработки.")

    # Вывод параметров
    return {
        # "message": f"Обработка {len(parquet_files)} файлов запущена.",
        # "files": parquet_files,
        "params_received": params.dict()
    }


@app.get("/list")
def list_all_files(name: str = ""):
    if len(name) == 0:
        files = []
    else:
        root = Path(DIR_DATA) / name
        files = [str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()]
    return {"files": files}

@app.get("/download")
def download_file(filename: str = Query(...)):
    file_path = Path(DIR_DATA) / filename
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Файл не найден")
    return FileResponse(file_path, filename=file_path.name, media_type="application/octet-stream")
