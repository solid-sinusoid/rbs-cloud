import requests
from pathlib import Path

# === Конфигурация ===
SERVER = "http://localhost:8000"
LIST_URL = f"{SERVER}/list"
DOWNLOAD_URL = f"{SERVER}/download"
DATASET_ROOT = "converted_dataset"
SAVE_ROOT = "downloaded_data"


def get_dataset(name:str):
    print("📥 Запрос списка файлов с сервера...")
    try:
        response = requests.get(LIST_URL, params={"name": name})
        response.raise_for_status()
        return response.json().get("files", [])
    except Exception as e:
        print("❌ Ошибка получения списка:", str(e))
        return []


def download_file(relative_path):
    save_path = Path(SAVE_ROOT) / relative_path
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


if __name__ == "__main__":
    files = get_dataset(DATASET_ROOT)
    if not files:
        print("❗️Нет файлов для загрузки.")
    else:
        for file_path in files:
            download_file(DATASET_ROOT + "/" +file_path)
