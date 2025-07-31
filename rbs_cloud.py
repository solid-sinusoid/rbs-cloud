import os
import shutil
from typing import List, Optional
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

@app.post("/save-dataset/")
async def save_dataset(dataset_name: str):
    """Finalize dataset creation by updating its status."""
    ds_path = Path(DIR_DATA) / dataset_name

    if not ds_path.exists():
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not created")

    # Обновляем статус датасета
    try:
        df = pd.read_parquet(DATASET_FILE)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    mask = df["name"] == dataset_name
    if not mask.any():
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' metadata not found")

    df.loc[mask, "status"] = DatasetStatus.SAVE
    df.to_parquet(DATASET_FILE, index=False, engine="pyarrow")

    return {"message": f"Dataset '{dataset_name}' saved"}

if not os.path.exists(WEIGHTS_FILE):
    schema = pa.schema([
        # ("id", pa.int32()),
        ("name", pa.string()),
        ("dataset", pa.string()),
        ("steps", pa.int32())
    ])
    table = pa.Table.from_pandas(pd.DataFrame(columns=schema.names), schema=schema)
    pq.write_table(table, WEIGHTS_FILE)

@app.get("/")
def root():
    return {"message": "Rbs Cloud (DuckDB Parquet API) работает!"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/create-dataset/")
async def create_dataset(dataset_name: str):
    ds_path = Path(DIR_DATA) / dataset_name
    # Проверим на повтор
    if ds_path.exists():
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
        # Создадим папку датасета
        ds_path.mkdir()
    except Exception as e:
        # В случае ошибки откатываем транзакцию
        con.execute("ROLLBACK;")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()

    return {"message": f"Dataset '{dataset_name}' added successfully"}

def safe_relative_path(rel_path: str) -> Path:
    """
    Проверка и нормализация относительного пути: запрещаем выход за пределы через '..'
    """
    p = Path(rel_path)
    if any(part == ".." for part in p.parts):
        raise ValueError("Относительный путь содержит запрещённые сегменты '..'")
    return p

def get_dataset_info(name: str) -> dict:
    df = duckdb.query(f"SELECT * FROM '{DATASET_FILE}' WHERE name = '{name}'").to_df()
    return df.to_dict(orient="records")

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
