import os
import subprocess
import logging
import pandas as pd
from datetime import datetime
import re
import glob  # Добавлен импорт glob
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Загрузка переменных окружения
load_dotenv()

# Пути к скриптам и папкам
TPO_API_DATA_PATH = os.getenv("PROCESSED_DATA_PATH", "data/tpo_api_data")
PAYMENTS_SUBFOLDER = "payments"
PAYMENT_ACTIONS_SUBFOLDER = "payment_actions"
ACTION_DETAILS_SUBFOLDER = "action_details"
PROCESSED_DATES_FILENAME = "processed_dates.csv"
EXTRACT_PAYMENTS_SCRIPT = "src/etl/extract/extract_travelpayouts_payments.py"
EXTRACT_PAYMENT_ACTIONS_SCRIPT = "src/etl/extract/extract_travelpayouts_payment_actions.py"
EXTRACT_ACTION_DETAILS_SCRIPT = "src/etl/extract/extract_travelpayouts_action_details.py"

# Минимальный год и месяц для обработки
MIN_YEAR_MONTH = "201801"

def is_period_processed(year_month):
    """Проверяет, был ли уже обработан указанный период."""
    processed_dates_filepath = os.path.join(TPO_API_DATA_PATH, PROCESSED_DATES_FILENAME)
    if os.path.exists(processed_dates_filepath):
        try:
            processed_dates_df = pd.read_csv(processed_dates_filepath)
            return year_month in processed_dates_df["year_month"].values
        except Exception as e:
            logging.error(f"Ошибка при чтении файла {PROCESSED_DATES_FILENAME}: {e}")
            return False
    return False

def mark_period_as_processed(year_month):
    """Добавляет информацию об обработанном периоде в файл."""
    processed_dates_filepath = os.path.join(TPO_API_DATA_PATH, PROCESSED_DATES_FILENAME)
    new_data = {"year_month": [year_month]}
    new_df = pd.DataFrame(new_data)

    if os.path.exists(processed_dates_filepath):
        try:
            existing_df = pd.read_csv(processed_dates_filepath)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df.to_csv(processed_dates_filepath, index=False, encoding='utf-8')
            logging.info(f"Добавлена информация об обработанном периоде {year_month} в файл {PROCESSED_DATES_FILENAME}")
        except Exception as e:
            logging.error(f"Ошибка при обновлении файла {PROCESSED_DATES_FILENAME}: {e}")
    else:
        try:
            new_df.to_csv(processed_dates_filepath, index=False, encoding='utf-8')
            logging.info(f"Создан файл {PROCESSED_DATES_FILENAME} и добавлена информация об обработанном периоде {year_month}")
        except Exception as e:
            logging.error(f"Ошибка при создании файла {PROCESSED_DATES_FILENAME}: {e}")

def run_script(script_path, *args):
    """Запускает указанный Python скрипт с аргументами."""
    try:
        logging.info(f"Запускаем скрипт: {script_path} с аргументами: {args}")
        subprocess.run(["python", script_path, *args], check=True)
        logging.info(f"Скрипт {script_path} успешно выполнен.")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Ошибка при выполнении скрипта {script_path}: {e}")
        return False
    except FileNotFoundError:
        logging.error(f"Скрипт не найден: {script_path}")
        return False

def extract_year_month_from_filename(filename):
    """Извлекает год и месяц из имени файла в формате tpo_payments_YYYYMM_EXTRACTED.csv."""
    match = re.search(r"tpo_payments_(\d{6})_EXTRACTED\.csv", filename)
    if match:
        return match.group(1)
    else:
        return "" #Возвращаем пустую строку

if __name__ == "__main__":
    logging.info("Запускаем ETL pipeline...")

    # 1. Извлекаем данные о выплатах (extract_travelpayouts_payments.py)
    if run_script(EXTRACT_PAYMENTS_SCRIPT):
        logging.info("Извлечение выплат успешно завершено.")

        # 2. Получаем список файлов выплат
        payments_folder_path = os.path.join(TPO_API_DATA_PATH, PAYMENTS_SUBFOLDER)
        payments_csv_files = [f for f in glob.glob(os.path.join(payments_folder_path, "tpo_payments_*.csv")) if "payments_metadata" not in f]

        # Сортируем файлы по дате
        payments_csv_files.sort(key=extract_year_month_from_filename)
        logging.info("Файлы выплат отсортированы по дате.")

        # 3. Итерируемся по каждому файлу выплат
        for payments_csv_path in payments_csv_files:
            try:
                filename = os.path.basename(payments_csv_path)
                match = re.search(r"tpo_payments_(\d{6})_EXTRACTED\.csv", filename)
                if match:
                    year_month = match.group(1)
                    logging.info(f"Извлечен year_month из имени файла: {year_month}")

                    # Проверяем, был ли уже обработан этот период *и что он не раньше MIN_YEAR_MONTH*
                    if year_month >= MIN_YEAR_MONTH and not is_period_processed(year_month):
                        logging.info(f"Начинаем обработку периода: {year_month}")

                        # Формируем имя файла для действий
                        actions_csv_filename = f"tpo_payment_actions_{year_month}_EXTRACTED.csv"
                        actions_csv_filepath = os.path.join(TPO_API_DATA_PATH, PAYMENT_ACTIONS_SUBFOLDER, actions_csv_filename)

                        # 4. Запускаем скрипт извлечения действий (extract_payment_actions.py)
                        # ПЕРЕДАЕМ payments_csv_path как аргумент
                        if run_script(EXTRACT_PAYMENT_ACTIONS_SCRIPT, payments_csv_path):
                            logging.info("Извлечение действий успешно завершено.")

                            # 5. Запускаем скрипт извлечения детализации (extract_action_details.py)
                            #  ПЕРЕДАЕМ actions_csv_filepath как аргумент
                            if run_script(EXTRACT_ACTION_DETAILS_SCRIPT, actions_csv_filepath):
                                logging.info("Извлечение детализации успешно завершено.")

                                # 6. Отмечаем период как обработанный
                                mark_period_as_processed(year_month)
                            else:
                                logging.error("Не удалось извлечь детализацию.")
                        else:
                            logging.error("Не удалось извлечь действия.")
                    else:
                        logging.info(f"Период {year_month} уже был обработан или раньше {MIN_YEAR_MONTH}. Пропускаем.")
                else:
                    logging.error(f"Не удалось извлечь year_month из имени файла: {filename}")
            except Exception as e:
                logging.error(f"Произошла ошибка при обработке файла {payments_csv_path}: {e}")
    else:
        logging.error("Не удалось извлечь выплаты.")

    logging.info("ETL pipeline завершен.")