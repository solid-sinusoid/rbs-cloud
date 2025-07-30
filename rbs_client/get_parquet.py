import requests
import argparse

DATA_SERVER = "http://localhost:8000"

def fetch_preview(file: str, limit: int):
    url = f"{DATA_SERVER}/preview?file={file}&limit={limit}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Проверка на ошибки HTTP

        # Получаем данные в формате JSON
        data = response.json()
        
        # Выводим данные
        for record in data:
            print(record)
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch preview from the server.")
    parser.add_argument("--file", type=str, default="file_ids", help="Имя файла (без расширения)")
    parser.add_argument("--limit", type=int, default=10, help="Количество записей для получения")

    args = parser.parse_args()

    fetch_preview(file=args.file, limit=args.limit)
    # fetch_preview("datasets",25)
