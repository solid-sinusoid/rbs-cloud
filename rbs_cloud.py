import os
import shutil
from typing import List, Optional
from pathlib import Path

import duckdb
import pandas as pd
from pydantic import BaseModel
from fastapi import FastAPI, Query, HTTPException, File, UploadFile
from fastapi.responses import FileResponse

app = FastAPI()

# Папка с файлами БД - .parquet
DIR_DATA = "data"
# Папка кэша
DIR_CACHE = "cache"
# Путь к parquet-файлу
ID_FILE = DIR_DATA + "/file_ids.parquet"

# Проверка наличия файла при старте
if not os.path.exists(ID_FILE):
    # create id_file
    df = pd.DataFrame({
        "name": ["RBS_ID"],
        "cid": 0,
        "sign": [""]
    })
    df.to_parquet(ID_FILE, index=False)


@app.get("/")
def root():
    return {"message": "DuckDB Parquet API работает!"}


@app.get("/preview")
def preview(limit: int = 10):
    try:
        df = duckdb.query(f"SELECT * FROM '{ID_FILE}' LIMIT {limit}").to_df()
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
