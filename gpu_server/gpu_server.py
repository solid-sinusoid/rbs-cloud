import asyncio, datetime as dt, os, shutil, subprocess, tarfile, uuid
from pathlib import Path
from typing import Dict, Literal, Optional

import requests
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel, Field

# ─────────────────── Константы центрального сервера ───────────────────
DATA_SERVER   = "http://msi.lan:8000"
LIST_URL      = f"{DATA_SERVER}/list"      # GET  → {"files": ["rbs_ros2bag", ...]}
DOWNLOAD_URL  = f"{DATA_SERVER}/download"  # GET  dataset_name+".zip"
UPLOAD_URL    = f"{DATA_SERVER}/upload"    # POST multipart/form-data

# ────────────────── Локальные директории GPU‑узла ─────────────────────
DATA_DIR = Path("data")   # <datasets>/<dataset_name>/…
JOBS_DIR = Path("jobs")       # <jobs>/<job_id>/{output,train.log,model.tar.gz}
DATA_DIR.mkdir(exist_ok=True)
JOBS_DIR.mkdir(exist_ok=True)

# ──────────────────────── Pydantic‑модели ─────────────────────────────
class TrainRequest(BaseModel):
    dataset_name : str
    steps        : int  = 100_000
    device       : Literal["cpu", "cuda"] = "cuda"
    repo_id      : str  = "rbs_ros2bag"
    use_vae      : bool = True
    root_override: Optional[str] = None

class JobStatus(BaseModel):
    job_id    : str
    state     : Literal["pending", "running", "finished", "failed"] = "pending"
    created_at: dt.datetime
    updated_at: dt.datetime
    progress  : Optional[float] = None   # 0–100
    message   : Optional[str] = None

# ────────────────────────── Runtime‑storage ──────────────────────────
JOBS: Dict[str, JobStatus] = {}

# ──────────────────────────── FastAPI app ────────────────────────────
app = FastAPI(title="GPU Training Orchestrator")

# ──────────────────────── Вспомогательные функции ────────────────────
def _update_job(job_id: str, **kw):
    job = JOBS[job_id]
    for k, v in kw.items(): setattr(job, k, v)
    job.updated_at = dt.datetime.utcnow()

def download_dataset(relative_path: Path):
    """"""
    save_path = Path(DATA_DIR) / relative_path
    save_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"⬇️  Загрузка: {relative_path}")
    try:
        response = requests.get(DOWNLOAD_URL, params={"filename": str(relative_path)}, stream=True)
        response.raise_for_status()
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✅ Сохранено: {save_path}")
    except Exception as e:
        print(f"❌ Ошибка загрузки {relative_path}: {str(e)}")

def upload_weights(job_id: str, output_dir: Path):
    """Архивируем output_dir и отсылаем на центральный сервер."""
    tar_path = output_dir.with_suffix(".tar.gz")
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(output_dir, arcname=".")
    with open(tar_path, "rb") as f:
        r = requests.post(UPLOAD_URL, files={"file": (tar_path.name, f)}, timeout=30)
        r.raise_for_status()                # 4xx/5xx => исключение
    tar_path.unlink()                       # убираем архив

async def run_training(files: list[Path], job_id: str, req: TrainRequest):
    job_root   = JOBS_DIR / job_id
    log_path   = job_root / "train.log"
    output_dir = job_root / "output"
    job_root.mkdir(parents=True, exist_ok=True)

    # ---------- helper для записи в лог ----------
    def write_log(msg: str):
        ts = dt.datetime.utcnow().isoformat(sep=" ", timespec="seconds")
        with open(log_path, "a") as f:
            f.write(f"[{ts}] {msg}\n")
            f.flush()

    try:
        # 1. Dataset
        local_ds = DATA_DIR / req.dataset_name
        if not local_ds.exists():
            _update_job(job_id, message="Скачиваем датасет…")
            write_log("Downloading dataset")
            for file_path in files:
                download_dataset(req.dataset_name + "/" + file_path)

        # 2. Команда обучения
        cmd = [
            "python", "-m", "lerobot.scripts.train",
            "--dataset.repo_id", req.repo_id,
            "--dataset.root", req.root_override or str(local_ds),
            "--policy.type=act",
            f"--output_dir={output_dir}",
            f"--job_name={job_id}",
            f"--policy.device={req.device}",
            f"--policy.use_vae={str(req.use_vae).lower()}",
            f"--steps={req.steps}",
            f"--policy.repo_id={req.repo_id}",
        ]
        write_log("CMD: " + " ".join(cmd))

        # 3. Запуск
        _update_job(job_id, state="running", message="Запущено обучение")
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,   # объединяем stderr в stdout
        )

        async for raw in proc.stdout:           # raw = bytes
            line = raw.decode("utf-8", errors="replace").rstrip()
            write_log(line)
            if "step" in line and "/" in line:
                try:
                    cur, total = map(int, line.split("step")[1].split()[0].split("/"))
                    _update_job(job_id, progress=cur * 100 / total)
                except Exception:
                    pass

        rc = await proc.wait()
        if rc != 0:
            raise RuntimeError(f"Процесс вернул код {rc}")

        # 5. Выгрузка весов ⟶ NAS
        _update_job(job_id, message="Загружаем веса на NAS…", progress=100.0)
        write_log("Uploading weights to NAS")
        upload_weights(job_id, output_dir)

        # 6. Очистка
        shutil.rmtree(local_ds,  ignore_errors=True)
        shutil.rmtree(output_dir, ignore_errors=True)
        write_log("Cleanup complete")

        _update_job(job_id, state="finished", message="Готово. Веса загружены, данные удалены")

    except Exception as e:
        write_log(f"ERROR: {e}")
        _update_job(job_id, state="failed", message=str(e))


def get_dataset(name:str):
    print("📥 Запрос списка файлов с сервера...")
    try:
        response = requests.get(LIST_URL, params={"name": name})
        response.raise_for_status()
        return response.json().get("files", [])
    except Exception as e:
        print("❌ Ошибка получения списка:", str(e))
        return []

# ───────────────────────────── API ─────────────────────────────
@app.post("/train")
async def train(req: TrainRequest, bg: BackgroundTasks):
    # проверяем наличие датасета на NAS
    files = get_dataset(req.dataset_name)
    if not files:
        raise HTTPException(404, "Такой датасет не найден на NAS")

    job_id = uuid.uuid4().hex[:8]
    JOBS[job_id] = JobStatus(job_id=job_id, created_at=dt.datetime.utcnow(), updated_at=dt.datetime.utcnow())
    bg.add_task(run_training, files, job_id, req)
    return {"job_id": job_id, "status_url": f"/status/{job_id}"}

@app.get("/status/{job_id}")
def status(job_id: str):
    job = JOBS.get(job_id) or HTTPException(404, "Job not found")
    return job

@app.get("/log/{job_id}")
def log(job_id: str, lines: int = 50):
    lp = JOBS_DIR / job_id / "train.log"
    if not lp.exists(): raise HTTPException(404, "Лог ещё не создан")
    return {"log_tail": "".join(lp.read_text().splitlines()[-lines:])}

@app.get("/")
def root(): return {"message": "GPU сервер доступен"}
