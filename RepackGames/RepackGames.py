import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
import json
import re
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse


class RepackGamesParser:
    def __init__(self, max_concurrent=160):
        self.base_url = "https://repack-games.com"
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Категории игр из инструкций
        self.categories = {
            "Action": "https://repack-games.com/category/action-games/",
            "Anime": "https://repack-games.com/category/anime-games/",
            "Adventure": "https://repack-games.com/category/adventure-games/",
            "Building": "https://repack-games.com/category/building-games/",
            "3D COMICS": "https://repack-games.com/category/3d-comics/",
            "EXPLORATION": "https://repack-games.com/category/exploration/",
            "EMULATOR GAMES": "https://repack-games.com/category/emulator-games/",
            "MULTIPLAYER": "https://repack-games.com/category/multiplayer-games/",
            "OPEN WORLD": "https://repack-games.com/category/open-world-game/",
            "VR-GAMES": "https://repack-games.com/category/vr-games/",
            "FIGHTING": "https://repack-games.com/category/fighting-games/",
            "NUDITY": "https://repack-games.com/category/nudity/",
            "HORROR": "https://repack-games.com/category/horror-games/",
            "RACING": "https://repack-games.com/category/racing-game/",
            "SHOOTER": "https://repack-games.com/category/shooting-games/",
            "RPG": "https://repack-games.com/category/rpg-pc-games/",
            "PUZZLE": "https://repack-games.com/category/puzzle/",
            "SPORT": "https://repack-games.com/category/sport-game/",
            "SURVIVAL": "https://repack-games.com/category/survival-games/",
            "SIMULATION": "https://repack-games.com/category/simulation-game/",
            "STRATEGY": "https://repack-games.com/category/strategy-games/",
            "SCI-FI": "https://repack-games.com/category/sci-fi-games/"
        }
        
        self.games_data = []
        self.total_games_parsed = 0
        self.start_time = None

    async def get_page_content(self, session, url):
        """Получает содержимое страницы с повторными попытками (еще быстрее)"""
        async with self.semaphore:
            for attempt in range(3):
                try:
                    # Очень маленькая пауза, чтобы избежать пачек
                    await asyncio.sleep(0.01)
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as response:
                        response.raise_for_status()
                        return await response.text()
                except Exception as e:
                    if attempt < 2:
                        await asyncio.sleep(0.15)
                        continue
                    return None

    def is_404_page(self, html_content):
        """Проверяет, является ли страница 404 ошибкой"""
        if not html_content:
            return True
        soup = BeautifulSoup(html_content, 'html.parser')
        wrap_content = soup.find('div', class_='wrap-content')
        if wrap_content:
            article_btn = wrap_content.find('div', class_='article-btn')
            if article_btn and "Error 404" in article_btn.get_text():
                return True
        return False

    def parse_file_size(self, game_info):
        """Улучшенный парсинг размера файла"""
        if not game_info:
            return "Unknown"
        
        info_text = game_info.get_text()
        
        # Различные варианты поиска размера
        size_patterns = [
            r'Game size:\s*([0-9.]+\s*[KMGT]B\+?)',
            r'Size:\s*([0-9.]+\s*[KMGT]B\+?)', 
            r'([0-9.]+\s*[KMGT]B\+?)',  # Простой поиск размера
        ]
        
        for pattern in size_patterns:
            match = re.search(pattern, info_text, re.IGNORECASE)
            if match:
                size = match.group(1).strip()
                # Очищаем от возможного мусора
                size = re.sub(r'[^0-9.KMGTB+ ]', '', size).strip()
                if size:
                    return size
        
        return "Unknown"

    def parse_date_info(self, soup):
        """Улучшенный парсинг даты с множественными источниками"""
        upload_date = None
        
        # Сначала ищем в game-info
        game_info = soup.find('div', class_='game-info')
        if game_info:
            info_text = game_info.get_text()
            
            # Различные форматы дат в game-info (UPDATED / PUBLISHED)
            date_patterns = [
                r'(?:UPDATED|PUBLISHED)\s+O[nm]\s*-\s*(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
                r'(?:UPDATED|PUBLISHED)\s*:\s*(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
                r'(?:UPDATED|PUBLISHED)\s+(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
                r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, info_text, re.IGNORECASE)
                if match:
                    date_str = match.group(1)
                    # Пробуем разные форматы дат
                    for date_format in ['%d-%m-%Y', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%y', '%d/%m/%y']:
                        try:
                            parsed_date = datetime.strptime(date_str, date_format)
                            # Если год меньше 30, считаем что это 20xx
                            if parsed_date.year < 30:
                                parsed_date = parsed_date.replace(year=parsed_date.year + 2000)
                            # Если год меньше 100, считаем что это 19xx или 20xx
                            elif parsed_date.year < 100:
                                if parsed_date.year < 50:
                                    parsed_date = parsed_date.replace(year=parsed_date.year + 2000)
                                else:
                                    parsed_date = parsed_date.replace(year=parsed_date.year + 1900)
                            
                            upload_date = parsed_date.isoformat() + ".000Z"
                            break
                        except ValueError:
                            continue
                    if upload_date:
                        break
        
        # Если дата не найдена в game-info, ищем в time-article
        if not upload_date:
            time_article = soup.find('div', class_='time-article updated')
            if time_article:
                time_text = time_article.get_text().strip()
                
                # Парсим относительное время
                relative_patterns = [
                    r'(\d+)\s+(year|years)\s+ago',
                    r'(\d+)\s+(month|months)\s+ago', 
                    r'(\d+)\s+(week|weeks)\s+ago',
                    r'(\d+)\s+(day|days)\s+ago',
                    r'(\d+)\s+(hour|hours)\s+ago',
                    r'(\d+)\s+(minute|minutes)\s+ago',
                    r'(\d+)\s+(second|seconds)\s+ago',
                ]
                
                for pattern in relative_patterns:
                    match = re.search(pattern, time_text, re.IGNORECASE)
                    if match:
                        value = int(match.group(1))
                        unit = match.group(2).lower()
                        
                        now = datetime.now()
                        
                        if 'year' in unit:
                            calculated_date = now - timedelta(days=value * 365)
                        elif 'month' in unit:
                            calculated_date = now - timedelta(days=value * 30)
                        elif 'week' in unit:
                            calculated_date = now - timedelta(weeks=value)
                        elif 'day' in unit:
                            calculated_date = now - timedelta(days=value)
                        elif 'hour' in unit:
                            calculated_date = now - timedelta(hours=value)
                        elif 'minute' in unit:
                            calculated_date = now - timedelta(minutes=value)
                        elif 'second' in unit:
                            calculated_date = now - timedelta(seconds=value)
                        else:
                            continue
                        
                        upload_date = calculated_date.isoformat() + ".000Z"
                        break
        
        return upload_date

    async def parse_game_info(self, session, game_url):
        """Парсит информацию об игре со страницы игры асинхронно"""
        try:
            html_content = await self.get_page_content(session, game_url)
            if not html_content:
                return None
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Извлечение названия
            title_tag = soup.find('h1', class_='article-title entry-title')
            title = title_tag.get_text().strip() if title_tag else "Unknown Title"
            
            # Улучшенный парсинг размера файла
            game_info = soup.find('div', class_='game-info')
            file_size = self.parse_file_size(game_info)
            
            # Улучшенный парсинг даты
            upload_date = self.parse_date_info(soup)
            
            # Извлечение ссылок на скачивание
            download_links = self.extract_download_links(soup)
            
            # Проверяем что получили ссылки на скачивание
            if not download_links:
                return None
            
            # Увеличиваем счетчик (без вывода, чтобы не мешать rich progress)
            self.total_games_parsed += 1
            
            return {
                "title": title,
                "uris": download_links,
                "fileSize": file_size,
                "uploadDate": upload_date,
                "repackLinkSource": game_url
            }
        except Exception as e:
            print(f"❌ Ошибка парсинга игры {game_url}: {e}")
            return None

    def extract_download_links(self, soup):
        """Извлекает ссылки на скачивание, исключая первую ссылку под секцией TORRENT."""
        download_links = []

        # Собираем ссылки как с классом 'gp-download-buttons', так и 'enjoy-css'
        buttons_gp = soup.find_all('a', class_='gp-download-buttons')
        buttons_enjoy = soup.find_all('a', class_='enjoy-css')

        # Склеиваем списки в порядке появления на странице (BeautifulSoup сохраняет порядок в find_all)
        download_buttons = buttons_gp + buttons_enjoy

        is_torrent_section = False

        for button in download_buttons:
            # Определяем контекст (ищем метку TORRENT в текущем блоке и ближайших соседях)
            prev_texts = []
            current = button.parent

            for _ in range(5):
                if current is None:
                    break
                try:
                    prev_texts.append(current.get_text(strip=True).upper())
                except Exception:
                    pass
                current = current.find_previous_sibling()

            context_text = " ".join(prev_texts)

            # Пропускаем первую ссылку под секцией TORRENT, последующие берем
            if 'TORRENT' in context_text:
                if not is_torrent_section:
                    is_torrent_section = True
                    continue
                else:
                    is_torrent_section = False
            else:
                is_torrent_section = False

            href = button.get('href')
            if href and href not in download_links:
                download_links.append(href)

        return download_links

    def extract_game_links_from_category(self, soup):
        """Извлекает ссылки на игры из категории - КАК В 1.PY"""
        game_links = []
        
        # Ищем блок с контентом
        wrap_content = soup.find('div', class_='wrap-content')
        if not wrap_content:
            return game_links
        
        # Правильный способ: ищем все статьи в блоке articles-content
        articles_content = wrap_content.find('div', class_='articles-content')
        if articles_content:
            # Ищем все статьи
            articles = articles_content.find_all('article', class_='article')
            for article in articles:
                # Ищем заголовок с ссылкой
                title_element = article.find('h2', class_='article-title')
                if title_element:
                    link_element = title_element.find('a', href=True)
                    if link_element:
                        href = link_element['href']
                        if href.startswith('https://repack-games.com/') and '/category/' not in href and '/author/' not in href:
                            game_links.append(href)
        
        # Альтернативный способ поиска, если основной не сработал
        if not game_links:
            # Ищем все ссылки в контейнере modern-articles
            modern_articles = wrap_content.find('ul', class_='modern-articles')
            if modern_articles:
                articles = modern_articles.find_all('li')
                for article in articles:
                    link_tag = article.find('a', href=True)
                    if link_tag:
                        href = link_tag['href']
                        if href.startswith('https://repack-games.com/') and '/category/' not in href and '/author/' not in href:
                            game_links.append(href)
        
        # Третий способ - поиск во всех статьях без привязки к конкретному классу
        if not game_links:
            all_articles = wrap_content.find_all('article')
            for article in all_articles:
                # Ищем любые ссылки на игры в статье
                links = article.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if (href.startswith('https://repack-games.com/') and 
                        '/category/' not in href and 
                        '/author/' not in href and
                        '/?p=' not in href):  # Исключаем ссылки с параметрами
                        game_links.append(href)
                        break  # Берем только первую ссылку из каждой статьи
        
        return game_links

    def get_total_pages_from_pagination(self, soup):
        """Извлекает общее количество страниц из элемента пагинации"""
        wp_pagenavi = soup.find('div', class_='wp-pagenavi')
        if wp_pagenavi:
            pages_span = wp_pagenavi.find('span', class_='pages')
            if pages_span:
                text = pages_span.get_text()
                # Ищем паттерн "Page X of Y"
                match = re.search(r'Page \d+ of (\d+)', text)
                if match:
                    return int(match.group(1))
        return None

    async def parse_category_page_and_games(self, session, category_name, category_url, page):
        """Парсит страницу категории и сразу парсит найденные игры"""
        
        if page == 1:
            url = category_url
        else:
            url = f"{category_url}page/{page}/"
        
        html_content = await self.get_page_content(session, url)
        
        if self.is_404_page(html_content):
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Используем ИСПРАВЛЕННЫЙ метод поиска игр
        game_links = self.extract_game_links_from_category(soup)
        
        # без подробного спама в консоль
        
        if not game_links:
            return []
        
        # Парсим все игры с этой страницы параллельно
        tasks = [self.parse_game_info(session, game_url) for game_url in game_links]
        game_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Фильтруем успешные результаты
        valid_games = []
        for result in game_results:
            if isinstance(result, dict) and result is not None:
                valid_games.append(result)
        
        # без подробного спама в консоль
        return valid_games

    async def parse_category_with_pagination(self, session, category_name, category_url, progress):
        """СКОРОСТЬ И ПОИСК ИГР ИЗ 1.PY + динамический прогресс"""
        
        # Создаем задачу прогресса для этой категории
        task_id = progress.add_task(f"Processing category: {category_name}", total=None)
        
        # Сначала определяем общее количество страниц
        html_content = await self.get_page_content(session, category_url)
        if not html_content:
            progress.update(task_id, completed=True, description=f"[red]Failed category: {category_name}[/red]")
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        total_pages = self.get_total_pages_from_pagination(soup)
        
        if total_pages:
            max_pages = total_pages
        else:
            max_pages = 999  # Fallback до 404
        
        # Настраиваем прогресс: общее количество страниц и начальный 0/total
        completed_pages = 0
        try:
            progress.update(task_id, total=max_pages, description=f"Processing category: {category_name} (0/{max_pages})")
        except Exception:
            pass
        
        # Парсим первую страницу (уже загружена)
        all_games = []
        games_from_page = self.extract_game_links_from_category(soup)
        
        # без лишнего вывода
        
        if games_from_page:
            # Парсим игры с первой страницы
            tasks = [self.parse_game_info(session, game_url) for game_url in games_from_page]
            game_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            page_games = 0
            for result in game_results:
                if isinstance(result, dict) and result is not None:
                    all_games.append(result)
                    page_games += 1
            # без лишнего вывода
        
        # Первая страница обработана (даже если игр 0)
        completed_pages = 1
        try:
            progress.update(task_id, advance=1, description=f"Processing category: {category_name} ({completed_pages}/{max_pages})")
        except Exception:
            pass
        
        # Дальнейшее обновление будет в цикле страниц
        
        # Парсим остальные страницы ПАРАЛЛЕЛЬНО как в 1.py
        if max_pages > 1:
            remaining_pages = max_pages - 1
            
            # Создаем задачи для всех остальных страниц параллельно
            page_tasks = []
            for page in range(2, max_pages + 1):
                page_tasks.append(
                    self.parse_category_page_and_games(session, category_name, category_url, page)
                )
            
            # Выполняем все страницы параллельно с динамическим прогрессом
            for coro in asyncio.as_completed(page_tasks):
                try:
                    result = await coro
                    if isinstance(result, list):
                        all_games.extend(result)
                    completed_pages += 1
                    try:
                        progress.update(task_id, advance=1, description=f"Processing category: {category_name} ({completed_pages}/{max_pages})")
                    except Exception:
                        pass
                except Exception:
                    pass  # Игнорируем ошибки отдельных страниц
        
        # Завершаем, показывая финальный счетчик страниц
        try:
            progress.update(task_id, completed=True, description=f"Processing category: {category_name} ({completed_pages}/{max_pages})")
        except Exception:
            pass
        
        return all_games

    async def parse_all_categories(self):
        """Парсит все категории параллельно - визуал как в rutracker.py"""
        
        # Импортируем rich для красивого вывода как в rutracker.py
        try:
            from rich.console import Console
            from rich.progress import Progress, SpinnerColumn, TextColumn
            console = Console()
            
            # Прогресс как в rutracker.py  
            progress_bar = Progress(
                SpinnerColumn(),
                TextColumn("{task.description}"),
                console=console
            )
        except ImportError:
            # Fallback если rich не установлен
            class FakeProgress:
                def __init__(self, *args, **kwargs): pass
                def __enter__(self): return self
                def __exit__(self, *args): pass
                def add_task(self, desc, total=None): return 0
                def update(self, task_id, **kwargs): pass
            progress_bar = FakeProgress()
            console = type('obj', (object,), {'print': print})()
        
        self.start_time = time.time()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        # Ускоряем, увеличивая лимиты подключений
        connector = aiohttp.TCPConnector(
            limit=220,
            limit_per_host=60,
            ttl_dns_cache=300,
            use_dns_cache=True,
            enable_cleanup_closed=True,
            force_close=False,
            keepalive_timeout=30
        )
        
        # Используем созданный progress_bar
        with progress_bar as progress:
            
            async with aiohttp.ClientSession(
                headers=headers,
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=14)
            ) as session:
                
                # Создаем задачи для всех категорий (каждая категория сама добавит одну строку прогресса)
                tasks = [
                    self.parse_category_with_pagination(session, category_name, category_url, progress)
                    for category_name, category_url in self.categories.items()
                ]
                
                # Выполняем ВСЕ категории одновременно
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Собираем все игры в общий список
                for i, result in enumerate(results):
                    if isinstance(result, list):
                        self.games_data.extend(result)
                    elif isinstance(result, Exception):
                        category_name = list(self.categories.keys())[i]
                        console.print(f"[red]Ошибка категории {category_name}: {result}[/red]")
        
        elapsed_total = time.time() - self.start_time
        speed = len(self.games_data) / elapsed_total if elapsed_total > 0 else 0
        
        console.print(f"""
Обработка завершена успешно!
Всего игр: {len(self.games_data)}
Время: {elapsed_total:.1f} секунд
Скорость: {speed:.1f} игр/сек
""")

    async def save_to_json(self, filename="repackgames.json"):
        """Сохраняет данные в JSON файл асинхронно"""
        output_data = {
            "name": "Repack-Games",
            "downloads": self.games_data
        }
        
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(output_data, ensure_ascii=False, indent=2))
        
        print(f"Данные сохранены в файл: {filename}")

    async def run(self):
        """Запускает полный асинхронный парсинг"""
        try:
            await self.parse_all_categories()
            await self.save_to_json()
            print("\nПарсинг завершен успешно!")
        except KeyboardInterrupt:
            print("\nПарсинг прерван пользователем")
            if self.games_data:
                await self.save_to_json("partial_repack_games_data.json")
                print("Частичные данные сохранены")
        except Exception as e:
            print(f"Произошла ошибка: {e}")
            if self.games_data:
                await self.save_to_json("error_repack_games_data.json")
                print("Данные до ошибки сохранены")


async def main():
    # Ускоренные настройки
    parser = RepackGamesParser(max_concurrent=160)
    await parser.run()


if __name__ == "__main__":
    asyncio.run(main())