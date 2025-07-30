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
from fastapi import FastAPI, Query, HTTPException, File, UploadFile
from fastapi.responses import FileResponse

app = FastAPI()

# Папка с файлами БД - .parquet
DIR_DATA = "data"
# Папка кэша
DIR_CACHE = "cache"
# Пути к parquet-файлам
ID_FILE = DIR_DATA + "/file_ids.parquet"
DATASET_FILE = DIR_DATA + "/datasets.parquet"
WEIGHTS_FILE = DIR_DATA + "/weights.parquet"

class DatasetStatus(str, Enum):
    CREATING = "creating"
    CONVERSION = "conversion"
    STORE = "store"
    AT_WORK = "at work" # is weights

# Проверка наличия файла при старте
if not os.path.exists(ID_FILE):
    # create id_file
    df = pd.DataFrame({
        "name": ["RBS_ID", "datasets", "weights"],
        "cid": [0, 0, 0],
        "sign": ["", "", ""]
    })
    df.to_parquet(ID_FILE, index=False)

if not os.path.exists(DATASET_FILE):
    # Опишем схему
    schema = pa.schema([
        ("id", pa.int32()),
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
        ("id", pa.int32()),
        ("name", pa.string()),
        ("ref_up_ds", pa.int32()),
        ("steps", pa.int32())
    ])
    table = pa.Table.from_pandas(pd.DataFrame(columns=schema.names), schema=schema)
    pq.write_table(table, WEIGHTS_FILE)

@app.get("/")
def root():
    return {"message": "Rbs Cloud (DuckDB Parquet API) работает!"}

@app.post("/update-dataset/")
async def update_dataset(new_record: dict, update_id: int):
    # Открываем соединение с DuckDB
    con = duckdb.connect()

    try:
        # Начинаем транзакцию
        con.execute("BEGIN;")

        # Добавляем новую запись в DATASET_FILE
        new_df = pd.DataFrame([new_record])
        new_df.to_parquet(DATASET_FILE, index=False, engine='pyarrow', append=True)

        # Обновляем существующую запись в ID_FILE
        con.execute(f"""
            UPDATE '{ID_FILE}'
            SET cid = cid + 1
            WHERE name = 'RBS_ID';
        """)

        # Завершаем транзакцию
        con.execute("COMMIT;")
    except Exception as e:
        # В случае ошибки откатываем транзакцию
        con.execute("ROLLBACK;")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()

    return {"message": "Запись успешно добавлена и обновлена."}

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


@app.post("/upload")
def upload_parquet(file: UploadFile = File(...)):
    # if not file.filename.endswith(".parquet"):
    #     raise HTTPException(status_code=400, detail="Можно загружать только .parquet файлы.")
    
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
