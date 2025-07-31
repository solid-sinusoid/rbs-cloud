import requests
from pathlib import Path

# === Конфигурация ===
FOLDER_PATH = "/home/shalenikol/0/rbs_dataset_46ep_blue+black/rbs_bag/episode10/"
UPLOAD_URL = "http://localhost:8000/upload"
PROCESS_URL = "http://localhost:8000/process"

# === Параметры обработки ===
DATASET_NAME = Path(FOLDER_PATH).name

PROCESS_PARAMS = {
    "dataset_name": DATASET_NAME,
    "episode": 10,
    "mode": "rosbag",
}

def upload_parquet_files(folder_path):
    folder = Path(folder_path)
    files = list(folder.glob("*.*"))

    if not files:
        print("❗️Нет файлов в указанной папке.")
        return False

    for file_path in files:
        print(f"📤 Загружается: {file_path.name} ...")
        with open(file_path, "rb") as f:
            files = {"file": (file_path.name, f, "application/octet-stream")}
            response = requests.post(
                UPLOAD_URL, params={"dataset": DATASET_NAME}, files=files
            )

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
    success = upload_parquet_files(FOLDER_PATH)
    if success:
        trigger_processing(PROCESS_PARAMS)
