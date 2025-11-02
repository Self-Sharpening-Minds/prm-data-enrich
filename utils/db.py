import csv
import logging
from io import StringIO
from typing import Any

import pandas as pd
import psycopg2
from config import DatabaseConfig
from psycopg2.extras import RealDictCursor


class DatabaseManager:
    """Менеджер для работы с базой данных PostgreSQL.
    Обеспечивает подключение к БД, выполнение запросов, создание таблиц
    и другие операции с базой данных.
    Attributes:
        config (DatabaseConfig): Конфигурация подключения к БД
        connection: Соединение с базой данных
        logger: Логгер для записи событий
    """

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        """Инициализация менеджера базы данных.
        Args:
            config: Конфигурация подключения к БД. Если не указана,
                   используется конфигурация по умолчанию.
        """
        self.config = config or DatabaseConfig()
        self.connection = None
        self.logger = logging.getLogger(__name__)
        self._is_connected = False
        self._connect()

    def _connect(self) -> bool:
        """Установка соединения с базой данных.
        Returns:
            bool: True если подключение успешно, иначе False.
        """
        try:
            self.connection = psycopg2.connect(
                host=self.config.host,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                port=self.config.port
            )
            self._is_connected = True
            self.logger.debug(
                f"Успешное подключение к БД: {self.config.host}:"
                f"{self.config.port}/{self.config.database}"
            )
            return True
        except psycopg2.OperationalError as e:
            self.logger.error(f"Ошибка подключения к БД: {e}")
            self._is_connected = False
            return False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при подключении: {e}")
            self._is_connected = False
            return False

    def _execute_with_transaction(self, query: str, operation_name: str) -> bool:
        """Универсальный метод для выполнения SQL-запросов с обработкой транзакций.
        Args:
            query: SQL-запрос для выполнения
            operation_name: Название операции для логирования
        Returns:
            bool: True если операция успешна, иначе False.
        """
        if not self.connection:
            self.logger.error("Нет подключения к БД")
            return False

        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            cursor.close()
            self.logger.info(f"Операция '{operation_name}' выполнена успешно")
            return True
        except psycopg2.Error as e:
            self.logger.error(f"Ошибка при операции '{operation_name}': {e}")
            if self.connection:
                self.connection.rollback()
            return False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при операции '{operation_name}': {e}")
            return False

    def create_cleaned_table(self, source_table_name: str, new_table_name: str) -> bool:
        """Создание таблицы с очищенными данными.
        Создает новую таблицу с уникальными записями по telegram_id,
        оставляя только последние записи по fetch_date.
        Args:
            source_table_name: Имя исходной таблицы
            new_table_name: Имя новой таблицы с очищенными данными
        Returns:
            bool: True если таблица создана успешно, иначе False.
        """
        query = f"""
            DROP TABLE IF EXISTS {new_table_name};
            CREATE TABLE {new_table_name} AS
            SELECT DISTINCT ON ((data->>'telegram_id')::bigint) *
            FROM {source_table_name}
            WHERE data ? 'about'
            ORDER BY (data->>'telegram_id')::bigint, fetch_date DESC;
        """.strip()

        return self._execute_with_transaction(
            query,
            f"создание очищенной таблицы {new_table_name}"
        )

    def create_result_table(self, source_table_name: str,
                           result_table_name: str, drop_table: bool = False) -> bool:
        """Создание таблицы с результатами анализа данных.
        Args:
            source_table_name: Имя исходной таблицы
            result_table_name: Имя результирующей таблицы
            drop_table: Флаг необходимости удаления таблицы если существует
        Returns:
            bool: True если таблица создана успешно, иначе False.
        """
        if drop_table:
            drop_query = f"DROP TABLE IF EXISTS {result_table_name};"
            if not self._execute_with_transaction(
                drop_query, 
                f"удаление таблицы {result_table_name}"
            ):
                return False

        query = f"""
        CREATE TABLE {result_table_name} AS
        SELECT
            person_id::bigint AS person_id,
            fetch_date::timestamp without time zone AS fetch_date,
            (data->>'telegram_id')::bigint AS telegram_id,
            data->>'first_name' AS first_name,
            data->>'last_name' AS last_name,
            data->>'about' AS about,
            data->>'username' AS username,
            data->>'lang_code' AS lang_code,
            data->>'phone' AS phone,
            (data->>'bot')::boolean AS bot,
            (data->>'fake')::boolean AS fake,
            (data->>'scam')::boolean AS scam,
            (data->>'deleted')::boolean AS deleted,
            (data->>'premium')::boolean AS premium,
            (data->>'verified')::boolean AS verified,
            (data->>'restricted')::boolean AS restricted,
            (data->>'username_is_actual')::boolean AS username_is_actual,
            data->'personal_channel'->>'title' AS personal_channel_title,
            data->'personal_channel'->>'username' AS personal_channel_username,
            data->'personal_channel'->>'about' AS personal_channel_about,
            (data->'personal_channel'->>'channel_id')::bigint AS personal_channel_id,
            (data->'personal_channel'->>'participants_count')::integer
            AS personal_channel_participants_count,
            false AS valid,
            '' AS confidence,
            '' AS meaningful_first_name,
            '' AS meaningful_last_name,
            '' AS meaningful_about,
            ARRAY[]::text[] AS extracted_links,
            '' AS summary,
            ARRAY[]::text[] AS urls,
            ARRAY[]::text[] AS photos
        FROM {source_table_name}
        WHERE data ? 'about'
        AND {source_table_name}.person_id IN (
            SELECT telegram_id
            FROM public.channel_subscribers
            WHERE channel_id = -1002240495824
        )
        """

        return self._execute_with_transaction(
            query,
            f"создание результирующей таблицы {result_table_name}"
        )

    def test_connection(self) -> bool:
        """Тестирование подключения к базе данных.
        Returns:
            bool: True если подключение активно и тест пройден, иначе False.
        """
        if not self.connection:
            self.logger.warning("Нет активного подключения для тестирования")
            return False

        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT version(), current_database(), current_user")
            db_info = cursor.fetchone()
            cursor.close()

            if db_info:
                self.logger.info(
                    f"Тест подключения успешен. "
                    f"БД: {db_info[1]}, Пользователь: {db_info[2]}, "
                    f"Версия: {db_info[0].split(',')[0]}"
                )
            return True

        except psycopg2.Error as e:
            self.logger.error(f"Ошибка тестирования подключения: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при тестировании: {e}")
            return False

    def execute_query(self, query: str,
                      params: tuple | None = None
                      ) -> list[dict[str, Any]]:
        """Универсальный метод для выполнения произвольных SQL запросов.
        Args:
            query: SQL-запрос для выполнения
            params: Параметры для запроса
        Returns:
            List[Dict]: Результаты запроса в виде списка словарей
        """
        if not self.connection:
            self.logger.warning("Попытка выполнить запрос без активного подключения")
            return []

        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            self.logger.debug(f"Выполнение запроса: {query} с параметрами: {params}")
            cursor.execute(query, params)

            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                self.logger.info(f"Получено {len(results)} записей")
            else:
                self.connection.commit()
                results = [{"affected_rows": cursor.rowcount}]
                self.logger.debug(f"Запрос выполнен, затронуто строк: {cursor.rowcount}")

            cursor.close()
            return results
        except psycopg2.Error as e:
            self.logger.error(f"Ошибка выполнения запроса: {e}")
            self.connection.rollback()
            return []
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при выполнении запроса: {e}")
            return []

    def get_table_info(self, table_name: str) -> list[dict[str, Any]]:
        """Получение информации о структуре таблицы.
        Args:
            table_name: Имя таблицы
        Returns:
            List[Dict]: Информация о колонках таблицы
        """
        query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
        """
        return self.execute_query(query, (table_name,))

    def create_table_from_csv(self, csv_file_path: str, table_name: str,
                             delimiter: str = ',', encoding: str = 'utf-8') -> bool:
        """Создает таблицу в БД из CSV файла.
        Args:
            csv_file_path: Путь к CSV файлу
            table_name: Имя создаваемой таблицы
            delimiter: Разделитель в CSV файле
            encoding: Кодировка файла
        Returns:
            bool: True если таблица создана успешно, иначе False.
        """
        if not self.connection:
            self.logger.error("Нет подключения к БД")
            return False

        try:
            self.logger.info(f"Чтение CSV файла: {csv_file_path}")
            df = pd.read_csv(csv_file_path, delimiter=delimiter, encoding=encoding)
            self.logger.info(f"Прочитано {len(df)} строк, {len(df.columns)} колонок")
            self.logger.info(f"Колонки: {list(df.columns)}")

            drop_sql = f'DROP TABLE IF EXISTS "{table_name}";'
            create_table_sql = self._generate_create_table_sql(df, table_name)

            cursor = self.connection.cursor()
            cursor.execute(drop_sql)
            self.logger.info(f"Таблица {table_name} удалена (replace mode)")

            cursor.execute(create_table_sql)
            self.logger.info(f"Таблица {table_name} создана")

            if not df.empty:
                self._insert_data_from_dataframe(cursor, df, table_name)

            self.connection.commit()
            cursor.close()
            self.logger.info(f"Таблица {table_name} успешно создана из CSV файла")
            self.logger.info(f"Добавлено {len(df)} записей")
            return True

        except Exception as e:
            self.logger.error(f"Ошибка создания таблицы из CSV: {e}")
            if self.connection:
                self.connection.rollback()
            return False

    def _generate_create_table_sql(self, df: pd.DataFrame, table_name: str) -> str:
        """Генерирует SQL запрос для создания таблицы на основе DataFrame.
        Args:
            df: DataFrame с данными
            table_name: Имя таблицы
        Returns:
            str: SQL запрос для создания таблицы
        """
        type_mapping = {
            'int64': 'BIGINT',
            'float64': 'DOUBLE PRECISION',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'object': 'TEXT'
        }

        columns_sql = []
        for col_name, dtype in df.dtypes.items():
            pg_type = type_mapping.get(str(dtype), 'TEXT')
            columns_sql.append(f'"{col_name}" {pg_type}')

        create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'
        create_table_sql += ',\n'.join(columns_sql)
        create_table_sql += '\n);'

        return create_table_sql

    def _insert_data_from_dataframe(self, cursor: Any,
                                    df: pd.DataFrame, table_name: str
                                    ) -> None:
        """Вставляет данные из DataFrame в таблицу.
        Args:
            cursor: Курсор базы данных
            df: DataFrame с данными
            table_name: Имя таблицы для вставки
        """
        output = StringIO()
        df.to_csv(output, sep='\t', header=False, index=False,
                  quoting=csv.QUOTE_NONE, escapechar='\\')
        output.seek(0)
        cursor.copy_from(output, table_name, null='', sep='\t')
        self.logger.info(f"Данные скопированы в таблицу {table_name}")

    def close(self) -> None:
        """Закрытие соединения с базой данных."""
        if self.connection:
            try:
                self.connection.close()
                self._is_connected = False
                self.logger.info("Соединение с БД успешно закрыто")
            except Exception as e:
                self.logger.error(f"Ошибка при закрытии соединения: {e}")
        else:
            self.logger.debug("Соединение уже закрыто или не было установлено")

    @property
    def is_connected(self) -> bool:
        """Проверка активности соединения с БД.
        Returns:
            bool: True если соединение активно, иначе False.
        """
        return self._is_connected
