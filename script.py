import sys
import os
from urllib.parse import urlparse, urljoin
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from PyQt6 import QtWidgets, QtCore, QtWidgets

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

errors = []


def is_valid_url(url):
    parsed = urlparse(url)
    return bool(parsed.scheme) and bool(parsed.netloc)


def get_links(url, update_output):
    update_output(f"Сканирование страницы: {url} ...")
    try:
        response = requests.get(url, headers=HEADERS, timeout=5)
        if response.status_code != 200:
            errors.append(
                f"Ошибка при сканировании страницы {url}, статус код: {response.status_code}"
            )
            return set(), set()

        content_type = response.headers.get("Content-Type", "")
        if "text/html" not in content_type:
            errors.append(
                f"Страница {url} не является HTML-документом (Content-Type: {content_type})"
            )
            return set(), set()

        soup = BeautifulSoup(response.text, "html.parser")
        internal_links = set()
        external_links = set()

        for link in soup.find_all("a", href=True):
            href = urljoin(url, link["href"])
            parsed_href = urlparse(href)

            if not parsed_href.scheme or not parsed_href.netloc:
                continue

            domain_parts = urlparse(url).netloc.split(".")
            domain = ".".join(domain_parts[-2:])

            if domain in parsed_href.netloc:
                internal_links.add(href.split("#")[0].rstrip("/"))
            else:
                external_links.add((url.split("#")[0].rstrip("/"), href))

        return internal_links, external_links
    except Exception as e:
        errors.append(f"Ошибка при сканировании страницы {url}: {e}")
        return set(), set()


def check_link(url, update_output):
    update_output(f"\tПереход по ссылке: {url} ...")
    try:
        response = requests.get(url, headers=HEADERS, allow_redirects=False, timeout=5)
        initial_status_code = response.status_code
        if initial_status_code == 301:
            redirected_url = response.headers.get("Location")
            return initial_status_code, redirected_url
        else:
            return initial_status_code, None
    except Exception as e:
        errors.append(f"Ошибка при проверке ссылки {url}: {e}")
        return None, None


def save_to_excel(
    data, output_folder_name, url_folder_name, mode_folder_name, columns, update_output
):
    try:
        setup_folder(output_folder_name)
        setup_folder(os.path.join(output_folder_name, url_folder_name))
        setup_folder(
            os.path.join(output_folder_name, url_folder_name, mode_folder_name)
        )
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        filename = f"{timestamp}.xlsx"
        filename_path = os.path.join(
            output_folder_name, url_folder_name, mode_folder_name, filename
        )

        if os.path.exists(filename_path):
            update_output(f"\tФайл '{filename_path}' уже существует. Запись отклонена.")
            return False
        else:
            df = pd.DataFrame(data, columns=columns)
            df.to_excel(filename_path, index=False)
            update_output(
                f"\tФайл '{filename}' успешно сохранен. Создано {len(data)} записей."
            )
            return True
    except Exception as e:
        update_output(f"\tОшибка при сохранении файла '{filename_path}': {e}")
        return False


def remove_duplicates(data):
    no_protocols_page_data = set()
    for pair in data:
        page_link = pair[0]
        no_protocol_page_link = urlparse(page_link).netloc + urlparse(page_link).path
        no_protocols_page_data.add((no_protocol_page_link, *pair[1:]))
    return no_protocols_page_data


def sort_links(data):
    return sorted(list(data), key=lambda pair: pair[0])


def add_indexes(data):
    return [(i + 1, *item) for i, item in enumerate(data)]


def crawl_website(base_url, mode, update_output, max_workers=5):
    visited = set()
    to_visit = {base_url}
    external_links = set()
    broken_links = set()
    redirected_links = set()
    checked_external_links = dict()

    lock = threading.Lock()

    def process_internal_links(url, update_output):
        with lock:
            if url in visited:
                return
            visited.add(url)

        internal_links, page_external_links = get_links(url, update_output)

        with lock:
            external_links.update(page_external_links)
            to_visit.update(internal_links - visited)

    def process_external_links(external_link, update_output):
        with lock:
            if external_link not in external_links:
                return
            external_links.remove(external_link)

        internal_url = external_link[0]
        external_url = external_link[1]

        with lock:
            if external_url in checked_external_links.keys():
                status_code, redirected_url = checked_external_links[external_url]
            else:
                status_code, redirected_url = check_link(external_url, update_output)
                checked_external_links[external_url] = (status_code, redirected_url)

        if status_code is None:
            return

        with lock:
            if mode == 2 and status_code not in [200, 301]:
                broken_links.add((internal_url, external_url))
            if mode == 3 and status_code == 301:
                redirected_links.add((internal_url, external_url, redirected_url))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        while to_visit or futures:
            while to_visit and len(futures) < max_workers:
                url = to_visit.pop()
                futures[url] = executor.submit(
                    process_internal_links, url, update_output
                )

            done = as_completed(futures.values())
            for future in done:
                for url, fut in futures.items():
                    if fut == future:
                        futures.pop(url)
                        break

    if mode == 2 or mode == 3:
        with ThreadPoolExecutor(max_workers=max_workers) as external_executor:
            external_futures = []

            for external_link in list(external_links):
                external_futures.append(
                    external_executor.submit(
                        process_external_links, external_link, update_output
                    )
                )

            for future in as_completed(external_futures):
                pass

    if mode == 1:
        return add_indexes(sort_links(remove_duplicates(external_links))), [
            "№",
            "Страница",
            "Адрес ссылки",
        ]
    elif mode == 2:
        return add_indexes(sort_links(remove_duplicates(broken_links))), [
            "№",
            "Страница",
            "Адрес ссылки",
        ]
    elif mode == 3:
        return add_indexes(sort_links(remove_duplicates(redirected_links))), [
            "№",
            "Страница",
            "Адрес ссылки",
            "Конечное перенаправление",
        ]


def setup_folder(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


class Worker(QtCore.QThread):
    update_output = QtCore.pyqtSignal(str)

    def __init__(self, website_url, mode, max_workers):
        super().__init__()
        self.website_url = website_url
        self.mode = mode
        self.max_workers = max_workers
        self.output_folder_name = "output"
        self.url_folder_name = website_url.split("//")[-1].replace("/", "-")

    def run(self):
        try:
            self.update_output.emit(
                f"\n\tЗапуск анализатора для {self.website_url} в режиме {self.mode}...\n"
            )

            data, columns = crawl_website(
                self.website_url,
                self.mode,
                self.update_output.emit,
                max_workers=self.max_workers,
            )

            if errors:
                self.update_output.emit("\nОшибки, возникшие во время выполнения:")
                for error in errors:
                    self.update_output.emit(f"\t{error}")

            if len(data) == 0:
                self.update_output.emit(
                    "\tНе найдено ни одной ссылки. Файл не будет сгенерирован."
                )
            else:
                if self.mode == 1:
                    external_links_folder_name = "external_links"
                    if save_to_excel(
                        data,
                        self.output_folder_name,
                        self.url_folder_name,
                        external_links_folder_name,
                        columns,
                        self.update_output.emit,
                    ):
                        self.update_output.emit(
                            f"\tТаблица с внешними ссылками сохранена в папку '{os.path.join( self.output_folder_name,  self.url_folder_name, external_links_folder_name)}'"
                        )
                elif self.mode == 2:
                    broken_links_folder_name = "broken_links"
                    if save_to_excel(
                        data,
                        self.output_folder_name,
                        self.url_folder_name,
                        broken_links_folder_name,
                        columns,
                        self.update_output.emit,
                    ):
                        self.update_output.emit(
                            f"\tТаблица с битыми ссылками сохранена в папку '{os.path.join( self.output_folder_name,  self.url_folder_name, broken_links_folder_name)}'"
                        )
                elif self.mode == 3:
                    redirected_links_folder_name = "redirected_links"
                    if save_to_excel(
                        data,
                        self.output_folder_name,
                        self.url_folder_name,
                        redirected_links_folder_name,
                        columns,
                        self.update_output.emit,
                    ):
                        self.update_output.emit(
                            f"\tТаблица с перенаправленными ссылками сохранена в папку '{os.path.join( self.output_folder_name,  self.url_folder_name, redirected_links_folder_name)}'"
                        )
        except Exception as e:
            self.update_output.emit(f"Ошибка во время анализа: {str(e)}")


class WebsiteAnalyzerApp(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle("АНАЛИЗАТОР САЙТА")
        self.setGeometry(100, 100, 600, 400)

        screen = QtWidgets.QApplication.primaryScreen()
        screen_geometry = screen.geometry()
        size = self.geometry()
        self.move(
            (screen_geometry.width() - size.width()) // 2,
            (screen_geometry.height() - size.height()) // 2,
        )

        main_layout = QtWidgets.QVBoxLayout()

        top_layout = QtWidgets.QHBoxLayout()

        self.url_label = QtWidgets.QLabel("Введите адрес сайта:")
        self.url_input = QtWidgets.QLineEdit(self)
        self.url_input.setPlaceholderText("http://example.com")
        top_layout.addWidget(self.url_label)
        top_layout.addWidget(self.url_input)

        self.threads_label = QtWidgets.QLabel("Потоки:")
        self.threads_spinbox = QtWidgets.QSpinBox(self)
        self.threads_spinbox.setMinimum(1)
        self.threads_spinbox.setMaximum(50)
        self.threads_spinbox.setValue(5)

        top_layout.addWidget(self.threads_label)
        top_layout.addWidget(self.threads_spinbox)

        main_layout.addLayout(top_layout)

        mode_layout = QtWidgets.QHBoxLayout()

        self.mode_label = QtWidgets.QLabel("Выберите режим работы:")
        self.mode_combo = QtWidgets.QComboBox(self)
        self.mode_combo.addItems(
            [
                "Только все внешние ссылки на всех внутренних страницах",
                "Битые ссылки (отличные от 200 и 301)",
                "Склеенные страницы (отдают 301)",
            ]
        )

        self.mode_combo.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Expanding,
            QtWidgets.QSizePolicy.Policy.Preferred,
        )

        mode_layout.addWidget(self.mode_label)
        mode_layout.addWidget(self.mode_combo)

        main_layout.addLayout(mode_layout)

        self.start_button = QtWidgets.QPushButton("Запустить анализ", self)
        self.start_button.clicked.connect(self.start_analysis)
        main_layout.addWidget(self.start_button)

        self.output_area = QtWidgets.QTextEdit(self)
        self.output_area.setReadOnly(True)
        main_layout.addWidget(self.output_area)

        self.setLayout(main_layout)

    def start_analysis(self):
        try:
            website_url = self.url_input.text().strip()
            mode = self.mode_combo.currentIndex() + 1
            max_workers = self.threads_spinbox.value()

            if not is_valid_url(website_url):
                self.output_area.append(
                    "Неверный URL. Пожалуйста, введите корректный адрес сайта, формата: http://example.com"
                )
                return

            self.worker = Worker(website_url, mode, max_workers)
            self.worker.update_output.connect(self.output_area.append)
            self.worker.start()
        except Exception as e:
            self.output_area.append(f"Ошибка во время анализа: {str(e)}")


def is_valid_url(url):
    regex = re.compile(
        r"^(?:http|https|ftp)://"
        r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}))"
        r"$",
        re.IGNORECASE,
    )
    return re.match(regex, url) is not None


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    analyzer = WebsiteAnalyzerApp()
    analyzer.show()
    sys.exit(app.exec())
