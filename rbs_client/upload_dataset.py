import os
import time
import requests
from requests.exceptions import RequestException

FOLDER_PATH = "/home/shalenikol/0/rbs_dataset_2025-06-03_aubo_sim/rbs_bag"

def check_server_available(server_url: str, timeout: float = 2.0, retries: int = 3, backoff: float = 1.0):
    health_url = server_url.rstrip("/") + "/health"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(health_url, timeout=timeout)
            if resp.status_code == 200:
                return True
        except RequestException:
            pass
        time.sleep(backoff * attempt)
    return False

def gather_files_with_relative_paths(root_dir: str):
    files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            full_path = os.path.join(dirpath, fname)
            rel_path = os.path.relpath(full_path, root_dir).replace("\\", "/")  # нормализация
            files.append((rel_path, full_path))
    return files

def upload_file(server_url: str, dataset_name: str, rel_path: str, full_path: str, max_retries: int = 2):
    url = server_url.rstrip("/") + "/upload-rel"
    for attempt in range(1, max_retries + 1):
        try:
            with open(full_path, "rb") as f:
                files = {
                    "file": (os.path.basename(rel_path), f, "application/octet-stream")
                }
                data = {
                    "dataset_name": dataset_name,
                    "relative_path": rel_path
                }
                resp = requests.post(url, data=data, files=files, timeout=60)
            if resp.status_code == 200:
                print(f"[OK] {rel_path}")
                return True
            else:
                print(f"[Ошибка {resp.status_code}] {rel_path}: {resp.text}")
        except RequestException as e:
            print(f"[Попытка {attempt}] Сетевая ошибка при загрузке {rel_path}: {e}")
        time.sleep(1 * attempt)
    print(f"[FAILED] Не удалось загрузить {rel_path} после {max_retries} попыток.")
    return False

def create_dataset(server_url: str, dataset_name: str) -> bool:
    url = f"{server_url.rstrip('/')}/create-dataset/"
    params = {"dataset_name": dataset_name}  # query-параметры

    try:
        resp = requests.post(url, params=params, timeout=10)
        resp.raise_for_status()  # выбросит ошибку для статуса 4xx/5xx
        # print("Успешно:", resp.json())
        return True
    except requests.RequestException as e:
        print("Ошибка запроса:", e)
        if resp is not None:
            print("Ответ сервера:", resp.status_code, resp.text)
        return False

def upload_dataset_to_server(dataset_name: str, dataset_root: str, server_url: str):
    if not check_server_available(server_url):
        print(f"Сервер {server_url} недоступен.")
        return

    if create_dataset(server_url, dataset_name):
        files = gather_files_with_relative_paths(dataset_root)
        if not files:
            print("Нет файлов для загрузки.")
            return

        success = 0
        for rel_path, full_path in files:
            if upload_file(server_url, dataset_name, rel_path, full_path):
                success += 1

        print(f"\nЗагружено {success}/{len(files)} файлов.")

# Пример запуска
if __name__ == "__main__":
    upload_dataset_to_server(
        dataset_name="my_dataset",
        dataset_root=FOLDER_PATH,
        server_url="http://localhost:8000"
    )
