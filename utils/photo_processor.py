import logging
from io import BytesIO
from pathlib import Path
from urllib.parse import urljoin

import config
import face_recognition
import numpy as np
import requests
from bs4 import BeautifulSoup
from PIL import Image
from sklearn.cluster import DBSCAN

logger = logging.getLogger(__name__)


class PhotoProcessor:
    """
    Класс для инкапсуляции логики по работе с фотографиями:
    извлечение, распознавание лиц и кластеризация.
    """

    def __init__(self, request_timeout: int = 3):
        """
        Инициализирует процессор фотографий.

        :param request_timeout: Таймаут для HTTP-запросов.
        """
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})
        self.timeout = request_timeout

    def _fetch_url_content(self, url: str) -> bytes | None:
        """
        Приватный метод для получения содержимого по URL.

        :param url: URL-адрес для запроса.
        :return: Содержимое ответа в байтах или None в случае ошибки.
        """
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            logger.debug(f"Ошибка запроса к URL {url}: {e}")
            return None

    def _get_image_data(self, source: str) -> bytes | None:
        """
        Получает бинарные данные изображения из источника.
        Источник может быть URL-адресом или локальным путем к файлу.

        :param source: URL или локальный путь.
        :return: Содержимое файла в байтах или None.
        """
        if source.startswith(('http://', 'https://')):
            return self._fetch_url_content(source)
        else:
            try:
                path = Path(source)
                if path.is_file():
                    return path.read_bytes()
                else:
                    logger.warning(f"Локальный файл не найден по пути: {source}")
                    return None
            except Exception as e:
                logger.error(f"Ошибка чтения локального файла {source}: {e}")
                return None

    def extract_image_urls_from_page(self, page_url: str) -> list[str]:
        """
        Извлекает все URL-адреса изображений с веб-страницы.

        :param page_url: URL-адрес страницы для сканирования.
        :return: Список уникальных URL-адресов изображений.
        """
        content = self._fetch_url_content(page_url)
        if not content:
            return []

        try:
            soup = BeautifulSoup(content, "html.parser")
            image_urls = set()

            for img_tag in soup.find_all("img"):
                src = img_tag.get("src") or img_tag.get("data-src")
                if not src:
                    continue

                full_url = urljoin(page_url, src)
                if any(full_url.lower().endswith(ext) for ext in config.IMAGE_EXTENSIONS):
                    image_urls.add(full_url)

            return list(image_urls)
        except Exception as e:
            logger.error(f"Ошибка анализа HTML для URL {page_url}: {e}")
            return []

    def _get_image_from_url(self, image_url: str) -> np.ndarray | None:
        """
        Загружает изображение по URL или локальному пути и конвертирует его в numpy-массив.

        :param image_url: URL-адрес изображения.
        :return: Изображение в формате numpy-массива или None.
        """
        content = self._get_image_data(image_url)
        if not content:
            return None

        try:
            image = Image.open(BytesIO(content)).convert("RGB")
            return np.array(image)
        except Exception as e:
            logger.warning(f"Ошибка обработки данных изображения из {image_url}: {e}")
            return None

    def is_single_human_face(self, image_url: str) -> bool:
        """
        Проверяет, содержит ли изображение ровно одно человеческое лицо.

        :param image_url: URL-адрес изображения.
        :return: True, если на изображении одно лицо, иначе False.
        """
        image = self._get_image_from_url(image_url)
        if image is None:
            return False

        try:
            bgr_image = image[:, :, ::-1]
            face_locations = face_recognition.face_locations(bgr_image, model="hog")
            return len(face_locations) == 1
        except Exception as e:
            logger.warning(f"Ошибка распознавания лиц для {image_url}: {e}")
            return False

    def get_face_embedding(self, image_url: str) -> np.ndarray | None:
        """
        Получает эмбеддинг (векторное представление) лица с изображения.

        :param image_url: URL-адрес изображения.
        :return: Эмбеддинг лица или None.
        """
        image = self._get_image_data(image_url)
        if image is None:
            return None

        try:
            bgr_image = self._get_image_from_url(image_url)
            bgr_image = bgr_image[:, :, ::-1]
            face_locations = face_recognition.face_locations(bgr_image, model="hog")
            if not face_locations:
                logger.debug(f"Лица не были найдены на {image_url} на этапе кодирования.")
                return None

            image = face_recognition.load_image_file(BytesIO(image))
            encodings = face_recognition.face_encodings(face_image=image, known_face_locations=face_locations)
            return encodings[0] if encodings else None
        except Exception as e:
            logger.warning(f"Ошибка кодирования лица для {image_url}")
            logger.debug(f"{e}")
            return None

    def cluster_faces(self, image_urls: list[str], eps: float = 0.6, min_samples: int = 2) -> list[list[str]]:
        """
        Кластеризует изображения с помощью алгоритма DBSCAN.

        :param image_urls: Список URL-адресов изображений для кластеризации.
        :param eps: Максимальное расстояние между двумя образцами, чтобы считать их соседями.
                    Это ваш предыдущий 'threshold'.
        :param min_samples: Минимальное количество образцов в окрестности для формирования кластера.
        :return: Список кластеров. Каждый кластер - это список URL-ов.
        """
        embeddings_map = {
            url: embedding
            for url in image_urls
            if (embedding := self.get_face_embedding(url)) is not None
        }

        if len(embeddings_map) < min_samples:
            return []

        urls = list(embeddings_map.keys())
        embeddings = np.array(list(embeddings_map.values()))

        db = DBSCAN(eps=eps, min_samples=min_samples, metric="euclidean").fit(embeddings)

        labels = db.labels_
        unique_labels = set(labels)

        clusters = []
        for label in unique_labels:
            if label == -1:
                continue

            indices = np.where(labels == label)[0]
            cluster = [urls[i] for i in indices]
            clusters.append(cluster)

        return clusters
