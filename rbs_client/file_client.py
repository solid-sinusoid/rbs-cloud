import requests

# путь к локальному parquet-файлу
file_path = "file_client.py"

# Адрес сервера
url = "http://localhost:8000/upload"

# Открытие и отправка
with open(file_path, "rb") as f:
    files = {"file": (file_path, f, "application/octet-stream")}
    response = requests.post(url, files=files)

# Ответ сервера
if response.ok:
    print("✅ Загружено:", response.json())
else:
    print("❌ Ошибка:", response.status_code, response.text)
