# def test_searching_photos() -> None:
#     """
#     Ищет, анализирует и кластеризует фото из веба и файлов.
#     Если кластер не найден, сохраняет локальные фото с лицами.
#     """
#     logger.info("Начинаем поиск и анализ фотографий.")

#     select_query = f"""
#         {config.SELECT_PERSONS_BASE_QUERY}
#         WHERE valid AND summary IS NOT NULL AND TRIM(summary) != ''
#         ORDER BY person_id
#     """

#     db = DatabaseManager()
#     try:
#         persons = db.execute_query(select_query)
#         if not persons:
#             logger.info("Не найдено персон для поиска фотографий.")
#             return

#         photo_processor = PhotoProcessor()
#         total = len(persons)

#         for i, person in enumerate(persons, 1):
#             person_id = person.get("person_id")
#             telegram_id = person.get("telegram_id")
#             person_urls = person.get("urls", [])

#             logger.info(f"[{i}/{total}] Обработка фотографий для person_id: {person_id}")

#             web_image_urls = []
#             if person_urls:
#                 for url in person_urls:
#                     web_image_urls.extend(photo_processor.extract_image_urls_from_page(url))
#                     if len(web_image_urls) > 50:
#                         web_image_urls = [] #TODO: когда слишком много фоток, алгоритм тупит
#                 logger.debug(f"Найдено {len(web_image_urls)} изображений в вебе для person_id: {person_id}")

#             local_avatars = []
#             avatars_dir = Path(config.PATH_PRM_MEDIA) / str(telegram_id) / config.PATH_PERSON_TG_AVATARS
#             if avatars_dir.is_dir():
#                 found_files = list(avatars_dir.glob('*.jpg'))
#                 local_avatars = [str(p) for p in found_files]
#                 logger.debug(f"Найдено {len(local_avatars)} локальных аватаров в {avatars_dir} для person_id: {person_id}")

#             web_human_face_images = [
#                 url for url in set(web_image_urls)
#                 if photo_processor.is_single_human_face(url)
#             ]

#             local_human_face_images = [
#                 path for path in set(local_avatars)
#                 if photo_processor.is_single_human_face(path)
#             ]

#             all_human_face_images = web_human_face_images + local_human_face_images
            
#             if not all_human_face_images:
#                 logger.warning(f"❌ Найдено {len(web_human_face_images)} веб-фото с лицом и {len(local_human_face_images)} локальных фото с лицом для person_id: {person_id}.")
#                 continue

#             logger.info(f"Найдено {len(web_human_face_images)} веб-фото с лицом и {len(local_human_face_images)} локальных фото с лицом для person_id: {person_id}.")

#             clusters = photo_processor.cluster_faces(all_human_face_images)
#             if not clusters:
#                 logger.warning("❌ Кластеры не сформированы. Проверяем наличие локальных фото с лицами.")
#                 if local_human_face_images:
#                     logger.info(f"❌✅ Сохраняем {len(local_human_face_images)} локальных фото с лицами как запасной вариант.")
#                     params = (local_human_face_images, person_id)
#                     db.execute_query(config.UPDATE_PHOTOS_QUERY, params)
#                 else:
#                     logger.info("❌❌ Локальных фото с лицами для сохранения не найдено.")
#                 continue

#             main_cluster = max(clusters, key=len)
#             if len(main_cluster) >= config.MIN_PHOTOS_IN_CLUSTER:
#                 params = (main_cluster, person_id)
#                 db.execute_query(config.UPDATE_PHOTOS_QUERY, params)
#                 logger.info(
#                     f"✅ Для {person_id} найден и сохранен кластер из {len(main_cluster)} фотографий."
#                 )
#             else:
#                 logger.warning(f"Самый большой кластер ({len(main_cluster)} фото) слишком мал. Проверяем локальные фото.")
#                 if local_human_face_images:
#                     logger.info(f"Сохраняем {len(local_human_face_images)} локальных фото с лицами вместо маленького кластера.")
#                     params = (local_human_face_images, person_id)
#                     db.execute_query(config.UPDATE_PHOTOS_QUERY, params)
#     finally:
#         db.close()
#     logger.info("Поиск и анализ фотографий завершен.")
