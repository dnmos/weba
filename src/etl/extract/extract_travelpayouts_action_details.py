import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
import logging
import argparse
import re
import time  # Импортируем модуль time

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='{asctime} - {levelname} - {message}', style='{')

# Загрузка переменных окружения из .env файла
load_dotenv()

# Получаем API токен и путь к папке processed из переменных окружения
API_TOKEN = os.getenv("TRAVELPAYOUTS_API_TOKEN")
PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH", "data/tpo_api_data")

# Определяем имена подпапок
ACTION_DETAILS_SUBFOLDER = "action_details"
PAYMENT_ACTIONS_SUBFOLDER = "payment_actions"

if not API_TOKEN:
    logging.error("Ошибка: TRAVELPAYOUTS_API_TOKEN не найден в переменных окружения.")
    exit()

# Создаем основные папки и подпапки, если они не существуют
os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
os.makedirs(os.path.join(PROCESSED_DATA_PATH, ACTION_DETAILS_SUBFOLDER), exist_ok=True)
os.makedirs(os.path.join(PROCESSED_DATA_PATH, PAYMENT_ACTIONS_SUBFOLDER), exist_ok=True)


def get_action_details(action_id, api_token, currency="usd"):
    """Получает детализацию по указанному action_id из API Travelpayouts."""
    API_URL = "https://api.travelpayouts.com/finance/v2/get_action_details"
    headers = {
        "X-Access-Token": api_token,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    params = {
        "action_id": action_id,
        "currency": currency # Используем USD
    }

    try:
        response = requests.get(API_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка запроса к API: {e}")
        if hasattr(response, 'status_code') and response.status_code == 404:
            logging.warning(f"Действие с ID {action_id} не найдено в API.")
        return None
    except ValueError as e:
        logging.error(f"Ошибка при разборе JSON: {e}")
        return None
    except Exception as e:
        logging.error(f"Произошла ошибка: {e}")
        return None


def process_action_details(action_details):
    """Преобразует детализацию действия в словарь для добавления в DataFrame."""
    if action_details is None:
        return None

    try:
        # Создаем словарь для хранения данных
        data = {}

        # Добавляем основные поля
        data['action_id'] = action_details.get('action_id')
        data['campaign_id'] = action_details.get('campaign_id')
        data['action_state'] = action_details.get('action_state')
        data['sub_id'] = action_details.get('sub_id')
        data['price'] = action_details.get('price')
        data['profit'] = action_details.get('profit')
        data['booked_at'] = action_details.get('booked_at')

        # Обрабатываем history
        if 'history' in action_details and action_details['history']:
            data['history_action_state'] = action_details['history'][0].get('action_state')
            data['history_price'] = action_details['history'][0].get('price')
            data['history_profit'] = action_details['history'][0].get('profit')
            data['history_profit_diff'] = action_details['history'][0].get('profit_diff')
            data['history_updated_at'] = action_details['history'][0].get('updated_at')
        else:
            data['history_action_state'] = None
            data['history_price'] = None
            data['history_profit'] = None
            data['history_profit_diff'] = None
            data['history_updated_at'] = None

        # Обрабатываем metadata
        if 'metadata' in action_details and action_details['metadata']:
            metadata_dict = {item['name']: item['value'] for item in action_details['metadata']}
            for key, value in metadata_dict.items():
                data[f'metadata_{key}'] = value
        else:
            pass

        return data

    except Exception as e:
        logging.error(f"Ошибка при обработке детализации действия: {e}")
        return None


def save_actions_details_to_csv(all_details, csv_filepath):
    """Сохраняет список детализаций действий в CSV файл."""
    if not all_details:
        logging.warning("Нет данных для сохранения.")
        return False

    try:
        df = pd.DataFrame(all_details)
        df.to_csv(csv_filepath, index=False, encoding='utf-8')
        logging.info(f"Детализация действий успешно сохранена в файл: {csv_filepath}")
        return True
    except Exception as e:
        logging.error(f"Ошибка при сохранении в CSV файл: {e}")
        return False


if __name__ == "__main__":
    logging.info("Начинаем извлечение детализации действий...")

    # Создаем парсер аргументов командной строки
    parser = argparse.ArgumentParser(description="Extract action details from a payment actions CSV file.")
    parser.add_argument("actions_csv_path", help="Path to the payment actions CSV file.")
    args = parser = argparse.ArgumentParser(description="Extract action details from a payment actions CSV file.")
    parser.add_argument("actions_csv_path", help="Path to the payment actions CSV file.")
    args = parser.parse_args()

    actions_csv_path = args.actions_csv_path

    logging.info(f"Обработка файла действий: {actions_csv_path}")

    try:
        # 4. Читаем CSV файл с действиями
        actions_df = pd.read_csv(actions_csv_path)
        logging.info(f"Файл действий успешно прочитан: {actions_csv_path}")
    except FileNotFoundError:
        logging.error(f"Ошибка: Файл {actions_csv_path} не найден.")
        exit()
    except Exception as e:
        logging.error(f"Ошибка при чтении CSV файла: {e}")
        exit()

    # 5. Проверяем наличие необходимых столбцов
    if actions_df.empty:
        logging.warning(f"Файл {actions_csv_path} пуст. Пропускаем.")
        exit()

    if "action_id" not in actions_df.columns:
        logging.error("Ошибка: В файле действий отсутствует необходимый столбец (action_id).")
        exit()

    # 6. Извлекаем YYYYMM из имени файла actions_csv_path
    try:
        filename = os.path.basename(actions_csv_path)
        match = re.search(r"tpo_payment_actions_(\d{6})_EXTRACTED\.csv", filename)
        if match:
            year_month = match.group(1)
            logging.info(f"Извлечен year_month из имени файла: {year_month}")
        else:
            logging.error("Не удалось извлечь year_month из имени файла.")
            exit()
    except Exception as e:
        logging.error(f"Ошибка при извлечении year_month из имени файла: {e}")
        exit()

    # 7. Создаем список для хранения детализаций всех действий
    all_action_details = []

    # 8. Итерируемся по каждому action_id в DataFrame
    for index, row in actions_df.iterrows():
        action_id = row["action_id"]

        logging.info(f"Обработка action_id: {action_id}")

        # Добавляем задержку
        time.sleep(0.1)  # Задержка в 0.1 секунды

        # 9. Получаем детализацию действия из API
        action_details = get_action_details(action_id, API_TOKEN, currency="usd") # Используем USD

        if action_details:
            processed_details = process_action_details(action_details)
            if processed_details:
                all_action_details.append(processed_details)
            else:
                logging.warning(f"Не удалось обработать детализацию действия для action_id: {action_id}")
        else:
            logging.warning(f"Не удалось получить детализацию действия для action_id: {action_id}")

    # 10. Определяем имя файла для сохранения детализации
    all_details_csv_filename = f"tpo_action_details_{year_month}_EXTRACTED.csv"
    # Сохраняем в подпапку action_details
    all_details_csv_filepath = os.path.join(PROCESSED_DATA_PATH, ACTION_DETAILS_SUBFOLDER, all_details_csv_filename)

    # 11. Сохраняем детализацию всех действий в один CSV файл
    if save_actions_details_to_csv(all_action_details, all_details_csv_filepath):
        logging.info("Извлечение детализации действий завершено.")
    else:
        logging.warning("Не удалось сохранить детализацию действий.")