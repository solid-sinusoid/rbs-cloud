import requests
import os

# === Конфигурация ===
SERVER = "http://localhost:8000"
LIST_URL = f"{SERVER}/list"
DOWNLOAD_URL = f"{SERVER}/download"
SAVE_FOLDER = "downloaded_parquet"

os.makedirs(SAVE_FOLDER, exist_ok=True)


def get_file_list():
    print("📥 Получение списка файлов с сервера...")
    try:
        response = requests.get(LIST_URL)
        response.raise_for_status()
        files = response.json().get("files", [])
        if not files:
            print("❗️Нет доступных файлов на сервере.")
        return files
    except Exception as e:
        print("❌ Ошибка при получении списка:", str(e))
        return []


def download_file(filename):
    print(f"⬇️  Загрузка файла: {filename}")
    try:
        response = requests.get(DOWNLOAD_URL, params={"filename": filename}, stream=True)
        response.raise_for_status()
        save_path = os.path.join(SAVE_FOLDER, filename)
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✅ Сохранено в {save_path}")
    except Exception as e:
        print("❌ Ошибка при загрузке:", str(e))


if __name__ == "__main__":
    files = get_file_list()
    if files:
        selected_file = files[0]  # или попросить пользователя выбрать
        download_file(selected_file)
