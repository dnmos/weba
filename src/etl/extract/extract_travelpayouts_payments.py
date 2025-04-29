import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
import re
import logging
import argparse  # Импортируем модуль argparse

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Загрузка переменных окружения из .env файла
load_dotenv()

# Получаем API токен из переменных окружения
API_TOKEN = os.getenv("TRAVELPAYOUTS_API_TOKEN")
if not API_TOKEN:
    logging.error("Ошибка: TRAVELPAYOUTS_API_TOKEN не найден в переменных окружения.")
    exit()


# URL API выплат Travelayouts (версия finance/v2/get_user_payments)
API_URL = "https://api.travelpayouts.com/finance/v2/get_user_payments"


# Заголовки запроса (включая X-Access-Token и User-Agent)
headers = {
    "X-Access-Token": API_TOKEN,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Определяем имя подпапки для выплат
PAYMENTS_SUBFOLDER = "payments"
PAYMENTS_METADATA_FILENAME = "payments_metadata.csv"

def extract_year_month_from_comment(comment):
    """
    Извлекает год и месяц из строки комментария.

    Args:
        comment (str): Строка комментария.

    Returns:
        str: Год и месяц в формате YYYYMM, или None, если не удалось извлечь.
    """
    if not isinstance(comment, str):
        return None

    # Шаблон для поиска месяца и года (учитывает разные варианты написания месяца)
    match = re.search(r"за (январь|февраль|март|апрель|май|июнь|июль|август|сентябрь|октябрь|ноябрь|декабрь)\s*(\d{4})", comment, re.IGNORECASE)

    if match:
        month_name = match.group(1).lower()
        year = match.group(2)

        # Преобразуем название месяца в номер с помощью словаря
        month_dict = {
            'январь': '01',
            'февраль': '02',
            'март': '03',
            'апрель': '04',
            'май': '05',
            'июнь': '06',
            'июль': '07',
            'август': '08',
            'сентябрь': '09',
            'октябрь': '10',
            'ноябрь': '11',
            'декабрь': '12'
        }

        month = month_dict.get(month_name)
        if month:
            return f"{year}{month}"
        else:
            logging.warning(f"Не удалось преобразовать месяц '{month_name}' в число.")
            return None
    else:
        return None


try:
    logging.info("Начинаем извлечение данных о выплатах...")

    # Отправка GET запроса к API
    response = requests.get(API_URL, headers=headers)

    # Проверка статуса ответа
    response.raise_for_status()  # Генерирует исключение для плохих ответов (4xx или 5xx)

    # Преобразование JSON ответа в Python список словарей
    data = response.json()

    # Преобразование данных в DataFrame pandas
    df = pd.DataFrame(data)

    # Определяем путь к папке tpo_api_data из .env или используем значение по умолчанию
    TPO_API_DATA_PATH = os.getenv("PROCESSED_DATA_PATH", "data/tpo_api_data")

    # Создаем основные папки и подпапки, если они не существуют
    os.makedirs(TPO_API_DATA_PATH, exist_ok=True)
    os.makedirs(os.path.join(TPO_API_DATA_PATH, PAYMENTS_SUBFOLDER), exist_ok=True)

    # Применяем функцию для извлечения года и месяца и сохраняем в новый столбец
    df['year_month'] = df['comment'].apply(extract_year_month_from_comment)

    # Фильтруем строки, где удалось извлечь год и месяц
    df = df[df['year_month'].notna()]

    # Проверяем, остались ли данные после фильтрации
    if df.empty:
        logging.warning("После фильтрации не осталось данных для сохранения.")
        exit() # Выходим из скрипта, если нет данных

    # Создаем список для хранения метаданных о выплатах
    payments_metadata = []

    # Сохраняем данные для каждого уникального year_month в отдельные файлы
    unique_year_months = df['year_month'].unique()
    for year_month in unique_year_months:
        # Фильтруем DataFrame для текущего year_month
        df_year_month = df[df['year_month'] == year_month]

        # Формируем имя CSV файла
        csv_filename = f"tpo_payments_{year_month}_EXTRACTED.csv"
        # Изменено: Сохраняем в подпапку payments
        csv_filepath = os.path.join(TPO_API_DATA_PATH, PAYMENTS_SUBFOLDER, csv_filename)

        # Сохраняем DataFrame в CSV файл
        df_year_month.to_csv(csv_filepath, index=False, encoding='utf-8')
        logging.info(f"Данные успешно сохранены в файл: {csv_filepath}")

        # Добавляем метаданные в список
        for index, row in df_year_month.iterrows():
            payments_metadata.append({
                "year_month": year_month,
                "payment_uuid": row["payment_uuid"],
                "filepath": csv_filepath
            })

    # Создаем DataFrame из списка метаданных
    metadata_df = pd.DataFrame(payments_metadata)

    # Формируем путь к файлу метаданных
    metadata_filepath = os.path.join(TPO_API_DATA_PATH, PAYMENTS_METADATA_FILENAME)

    # Сохраняем DataFrame с метаданными в CSV файл
    metadata_df.to_csv(metadata_filepath, index=False, encoding='utf-8')
    logging.info(f"Метаданные о выплатах сохранены в файл: {metadata_filepath}")


    logging.info("Извлечение данных о выплатах завершено.")


except requests.exceptions.RequestException as e:
    logging.error(f"Ошибка запроса к API: {e}")
except ValueError as e:
    logging.error(f"Ошибка при разборе JSON: {e}")
except Exception as e:
    logging.error(f"Произошла ошибка: {e}")