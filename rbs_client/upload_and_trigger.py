import requests
import argparse
from pathlib import Path

# === Конфигурация ===
FOLDER_PATH = "/home/shalenikol/0/rbs_dataset_2025-06-03_aubo_sim/rbs_bag/"
UPLOAD_URL = "http://localhost:8000/upload"
PROCESS_URL = "http://localhost:8000/process"

# === Параметры обработки ===
PROCESS_PARAMS = {
    "episode": 10,
    # "columns": ["id", "value"],
    "mode": "rosbag"
}

def upload_files(folder_path, ds_name):
    # folder = Path(folder_path)
    # files = list(folder.glob("*.*"))
    root = Path(folder_path)
    files = [str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()]

    if not files:
        print("❗️Нет файлов в указанной папке.")
        return False

    for rel_path in files:
        print(f"📤 Загружается: {rel_path} ...")
        file_path = root / rel_path
        with open(file_path, "rb") as f:
            files = {"file": (rel_path, f, "application/octet-stream")}
            response = requests.post(UPLOAD_URL, files=files, json={"dataset_name": ds_name})

        if response.ok:
            print("✅ Успешно:", response.json())
        else:
            print(f"❌ Ошибка при загрузке {file_path.name}: {response.status_code} - {response.text}")
            return False

    return True


def trigger_processing(params):
    print("🚀 Запрос на запуск обработки данных с параметрами...")
    try:
        response = requests.post(PROCESS_URL, json=params)
        if response.ok:
            print("✅ Обработка запущена:", response.json())
        else:
            print("❌ Ошибка запуска обработки:", response.status_code, response.text)
    except Exception as e:
        print("❌ Ошибка соединения с сервером:", str(e))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send dataset to the server.")
    parser.add_argument("--dataset", type=str, default="your_ds_name", help="Имя датасета (уникальное)")

    args = parser.parse_args()

    success = upload_files(FOLDER_PATH, args.dataset)
    if success:
        trigger_processing(PROCESS_PARAMS)
