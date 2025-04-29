import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
import logging
import argparse  # Добавлен import argparse
import re

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Загрузка переменных окружения из .env файла
load_dotenv()

# Получаем API токен и путь к папке processed из переменных окружения
API_TOKEN = os.getenv("TRAVELPAYOUTS_API_TOKEN")
PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH", "data/tpo_api_data")  # Путь по умолчанию

# Определяем имена подпапок
PAYMENT_ACTIONS_SUBFOLDER = "payment_actions"

if not API_TOKEN:
    logging.error("Ошибка: TRAVELPAYOUTS_API_TOKEN не найден в переменных окружения.")
    exit()

# Создаем основные папки и подпапки, если они не существуют
os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
os.makedirs(os.path.join(PROCESSED_DATA_PATH, PAYMENT_ACTIONS_SUBFOLDER), exist_ok=True)


def get_payment_actions(payment_uuid, api_token):
    """Получает список действий для указанной выплаты из API Travelpayouts."""
    API_URL = "https://api.travelpayouts.com/finance/v2/get_user_actions_affecting_payment"
    headers = {
        "X-Access-Token": api_token,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    params = {
        "payment_uuid": payment_uuid,
        "limit": 300  # Максимальное значение limit
    }

    try:
        response = requests.get(API_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка запроса к API: {e}")
        return None
    except ValueError as e:
        logging.error(f"Ошибка при разборе JSON: {e}")
        return None
    except Exception as e:
        logging.error(f"Произошла ошибка: {e}")
        return None


def save_actions_to_csv(actions_data, csv_filepath):
    """Сохраняет список действий в CSV файл."""
    if actions_data is None or "actions" not in actions_data:
        logging.warning("Нет данных для сохранения.")
        return False  # Возвращаем False, если нечего сохранять

    actions = actions_data["actions"]
    if not actions:
        logging.warning("Список действий пуст.")
        df = pd.DataFrame()  # Create an empty DataFrame to save
    else:
        df = pd.DataFrame(actions)

    try:
        df.to_csv(csv_filepath, index=False, encoding='utf-8')
        logging.info(f"Действия успешно сохранены в файл: {csv_filepath}")
        return True # Возвращаем True, если сохранение прошло успешно
    except Exception as e:
        logging.error(f"Ошибка при сохранении в CSV файл: {e}")
        return False # Возвращаем False, если произошла ошибка


def extract_payment_actions(payments_csv_path, api_token, processed_data_path):
    """Извлекает действия по выплатам из указанного CSV файла и сохраняет их."""
    logging.info(f"Обработка файла выплат: {payments_csv_path}")

    try:
        # 4. Читаем CSV файл с выплатами
        payments_df = pd.read_csv(payments_csv_path)
        logging.info(f"Файл выплат успешно прочитан: {payments_csv_path}")
    except FileNotFoundError:
        logging.error(f"Ошибка: Файл {payments_csv_path} не найден.")
        return False
    except Exception as e:
        logging.error(f"Ошибка при чтении CSV файла: {e}")
        return False

    # 5. Проверяем наличие необходимых столбцов
    if "payment_uuid" not in payments_df.columns or "year_month" not in payments_df.columns:
        logging.error("Ошибка: В файле выплат отсутствуют необходимые столбцы (payment_uuid или year_month).")
        return False

    # 6. Извлекаем YYYYMM из имени файла payments_csv_path
    try:
        filename = os.path.basename(payments_csv_path)
        match = re.search(r"tpo_payments_(\d{6})_EXTRACTED\.csv", filename)
        if match:
            year_month = match.group(1)
            logging.info(f"Извлечен year_month из имени файла: {year_month}")
        else:
            logging.error("Не удалось извлечь year_month из имени файла.")
            return False
    except Exception as e:
        logging.error(f"Ошибка при извлечении year_month из имени файла: {e}")
        return False

    # 7. Создаем список для хранения всех действий
    all_actions = []

    # 8. Итерируемся по каждой выплате в DataFrame
    for index, row in payments_df.iterrows():
        payment_uuid = row["payment_uuid"]

        logging.info(f"Обработка payment_uuid: {payment_uuid}, year_month: {year_month}")

        # 9. Получаем список действий для этой выплаты
        actions_data = get_payment_actions(payment_uuid, api_token)

        if actions_data and "actions" in actions_data:
            all_actions.extend(actions_data["actions"])  # Добавляем действия в общий список
        else:
            logging.warning(f"Не удалось получить список действий для payment_uuid: {payment_uuid}.")

    # 10. После обработки всех выплат, сохраняем все действия в один CSV файл для текущего месяца
    if all_actions:
        # 11. Формируем имя файла для сохранения действий
        actions_csv_filename = f"tpo_payment_actions_{year_month}_EXTRACTED.csv"
        # Изменено: Сохраняем в подпапку payment_actions
        actions_csv_filepath = os.path.join(processed_data_path, PAYMENT_ACTIONS_SUBFOLDER, actions_csv_filename)

        # 12. Сохраняем список действий в CSV файл
        if save_actions_to_csv({"actions": all_actions}, actions_csv_filepath):  # Создаем словарь для передачи в save_actions_to_csv
            logging.info(f"Действия для {year_month} успешно сохранены в файл: {actions_csv_filepath}")
        else:
            logging.warning(f"Не удалось сохранить действия для {year_month}.")
    else:
        logging.warning(f"Нет действий для сохранения в файле {year_month}.")
    return True


if __name__ == "__main__":
    logging.info("Начинаем извлечение данных о действиях по выплатам...")

    # Создаем парсер аргументов командной строки
    parser = argparse.ArgumentParser(description="Extract payment actions from a payments CSV file.")
    parser.add_argument("payments_csv_path", help="Path to the payments CSV file.")
    args = parser.parse_args()

    payments_csv_path = args.payments_csv_path

    # Вызываем функцию извлечения действий, передавая необходимые аргументы
    extract_payment_actions(payments_csv_path, API_TOKEN, PROCESSED_DATA_PATH)

    logging.info("Извлечение данных о действиях по выплатам завершено.")