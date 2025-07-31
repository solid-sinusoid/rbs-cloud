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
import requests
from fastapi import FastAPI, Query, HTTPException, File, UploadFile
from fastapi.responses import FileResponse

app = FastAPI()

# Папка с файлами БД - .parquet
DIR_DATA = "data"
# Папка кэша
DIR_CACHE = "cache"
# URL GPU-сервера для обучения
GPU_SERVER = os.environ.get("GPU_SERVER", "http://localhost:8001")
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

# def get_cid(con, name: str = 'datasets') -> int:
#     try:
#         # Выполняем SQL-запрос для получения cid
#         query = f"""
#             SELECT cid
#             FROM '{ID_FILE}'
#             WHERE name = '{name}'
#         """
#         result = con.execute(query).fetchone()  # Получаем первую запись

#         # Проверяем, найдена ли запись
#         if result is not None:
#             return result[0]  # Возвращаем значение cid
#         else:
#             raise ValueError(f"Запись с name='{name}' не найдена.")
#     except Exception as e:
#         print(f"Ошибка при получении cid: {e}")
#         return None

@app.get("/")
def root():
    return {"message": "Rbs Cloud (DuckDB Parquet API) работает!"}

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
def upload_parquet(
    file: UploadFile = File(...),
    dataset: str | None = None,
    steps: int | None = None,
):
    """Сохранение файла. Если указан dataset, файл кладётся в data/<dataset>."""
    if dataset:
        save_dir = Path(DIR_DATA) / dataset
    else:
        save_dir = Path(DIR_CACHE)
    save_dir.mkdir(parents=True, exist_ok=True)
    save_path = save_dir / file.filename

    try:
        with open(save_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        if dataset and dataset.startswith("weights/"):
            ds_name = dataset.split("/", 1)[1]
            try:
                df = pd.read_parquet(WEIGHTS_FILE)
            except Exception:
                df = pd.DataFrame()
            new_row = {
                "name": file.filename,
                "dataset": ds_name,
                "steps": steps or 0,
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            df.to_parquet(WEIGHTS_FILE, index=False)
        return {"message": f"Файл '{file.filename}' сохранён в {save_dir}/"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class ProcessParams(BaseModel):
    dataset_name: str
    episode: Optional[int] = None
    mode: Optional[str] = "convert"

@app.post("/process")
def process_uploaded_files(params: ProcessParams):
    """Простейшая конвертация файлов из rosbag в lerobot (заглушка)."""
    ds_dir = Path(DIR_DATA) / params.dataset_name
    if not ds_dir.exists():
        raise HTTPException(status_code=404, detail="Dataset not found")

    conv_dir = ds_dir / "converted"
    conv_dir.mkdir(exist_ok=True)
    bags = list(ds_dir.glob("*.bag"))
    for bag in bags:
        target = conv_dir / (bag.stem + ".lrb")
        shutil.copy(bag, target)

    # обновляем информацию о датасете
    try:
        df = pd.read_parquet(DATASET_FILE)
        idx = df.index[df["name"] == params.dataset_name]
        if len(idx) > 0:
            df.loc[idx, "num_episodes"] = len(bags)
            df.loc[idx, "status"] = DatasetStatus.STORE
            df.to_parquet(DATASET_FILE, index=False)
    except Exception:
        pass

    return {"message": f"Converted {len(bags)} files"}


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


@app.post("/train")
def start_training(dataset_name: str, steps: int = 100_000):
    """Проксирование запроса на GPU‑сервер."""
    try:
        resp = requests.post(
            f"{GPU_SERVER}/train",
            json={"dataset_name": dataset_name, "steps": steps},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/train-status/{job_id}")
def train_status(job_id: str):
    try:
        resp = requests.get(f"{GPU_SERVER}/status/{job_id}", timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
