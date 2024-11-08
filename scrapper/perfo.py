import datetime
from pathlib import Path
from scrapper.big_query import Big_query
import json
import socket
from contextlib import suppress
import atexit
import platform
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import time
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import traceback


def get_log_path(file_prefix, today):
    path = Path(__file__).parent.parent.joinpath('logs')
    path.mkdir(parents=True, exist_ok=True)
    filename = path.joinpath(f'{file_prefix}_{today}.txt')
    filename.touch(exist_ok=True)
    return filename


def add_log_line(line, filename):
    with open(filename, 'a', encoding='utf-8') as f:
        f.write(f"{line}\n")


class Perfo:
    LOGS = {}

    def __init__(self, log_to_file=True, log_to_console=None, log_file_name='erli_perfo_daily', log_prefix='PERFO'):
        self.log_prefix = log_prefix
        self.is_windows = is_windows()
        self.log_to_console = log_to_console if isinstance(log_to_console, bool) else self.is_windows  # <-- na pewno?
        self.log_to_file = log_to_file if isinstance(log_to_file, bool) else not self.is_windows
        self.today = datetime.datetime.today().strftime('%Y-%m-%d')
        self.yesterday = (datetime.datetime.today() - datetime.timedelta(1)).strftime("%Y-%m-%d")
        self.log_path = get_log_path(log_file_name, self.today)
        self.log_file_name = log_file_name
        self.bq = Big_query()
        if self.log_path not in self.LOGS:
            self.LOGS[self.log_path] = []
        else:
            if self.log_to_file:
                self.save_log_to_file()

        atexit.register(self.clean_logs)

    def clean_logs(self):
        if self.log_to_file:
            self.save_log_to_file()

    def log(self, line):
        line = f'{datetime.datetime.now().strftime("%H:%M:%S")} - {self.log_prefix}: {line}'
        if self.log_to_file:
            self.LOGS[self.log_path].append(line)
            # add_log_line(line, self.log_path)
        if self.log_to_console:
            print(line)
        session = requests.Session()
        retry = Retry(connect=5, backoff_factor=0.8)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('https://', adapter)
        session.post(
            'https://logs.collector.solarwinds.com/v1/log',
            headers={},
            json=f'[PERFO] {line}',
            auth=('', 'TOKEN DO PAPPERTRAILA'),
        )

    def save_log_to_file(self):
        if self.LOGS[self.log_path]:
            add_log_line("\n".join(self.LOGS[self.log_path]), self.log_path)
            self.LOGS[self.log_path] = []

    @staticmethod
    def get_table_schema(file_name: str) -> dict:
        with open(Path(Path(__file__).parent, 'table-schemas', file_name)) as f:
            return json.load(f)

    @staticmethod
    def get_query(file_name: str) -> str:
        with open(Path(Path(__file__).parent, 'queries', file_name)) as f:
            return f.read()

    @staticmethod
    def is_already_done_for_today(last_date, day_delta=1):
        if last_date:
            if isinstance(last_date, datetime.datetime):
                last_date = last_date.date()
            return last_date >= datetime.datetime.now().date() - datetime.timedelta(day_delta)
        return False

    def try_x_times(self, x, func, verbose=True, *args, **kwargs):
        for count in range(0, x):
            try:
                value = func(*args, **kwargs)
            except Exception as e:
                count += 1
                try:
                    self.log(f'ERROR {func.__name__} - {type(e).__name__} : {count}/{x}{f": {e.message}" if verbose else ""}')
                except AttributeError:
                    self.log(f'ERROR {func.__name__} - {type(e).__name__} : {count}/{x}{f": {e}" if verbose else ""}')
                if verbose:
                    self.log(traceback.format_exc())
                continue
            return value

        self.log(f'ERROR {func.__name__} : To many exeptions. Stop')
        if isinstance(self, Selenium_bot):
            try:
                self.screeshot_error()
                self.close()
                self.log('screenshot saved')
            except Exception as e:
                self.log(f'Cant take screenshot, somethng wrong: {e}')


class Selenium_bot:
    def __init__(self, debug=False):
        options = Options()
        self.is_windows = is_windows()
        self.debug = debug
        if not self.is_windows or not debug:
            options.add_argument("--headless")
            options.add_argument("--window-size=1834,880")
        if not debug:
            atexit.register(self.close)
        else:
            options.add_experimental_option("detach", True)
        options.add_argument("--no-sandbox")
        options.add_argument("start-maximized")
        options.add_argument("--disable-extensions")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        # sessions_path = Path(__file__).parent.joinpath('credentials').joinpath('sessions')
        # sessions_path.mkdir(exist_ok=True)
        # print(sessions_path)
        # options.add_argument(f"user-data-dir={sessions_path}")
        self.download_path = Path(__file__).parent.joinpath('download')
        self.download_path.mkdir(exist_ok=True)
        self.errors_screenshots_path = Path(__file__).parent.joinpath('errors_screenshots')
        self.errors_screenshots_path.mkdir(exist_ok=True)
        # print(f'download.default_directory={self.download_path}')
        # options.add_argument(f'download.default_directory={self.download_path}')
        prefs = {'download.default_directory': f'{self.download_path}'}
        options.add_experimental_option('prefs', prefs)
        self.clear_download_dir()
        if self.is_windows:
            self.driver = webdriver.Chrome(options=options)
        else:
            pass
            # chyba tak wystarczy
            self.driver = webdriver.Chrome(options=options, executable_path='/usr/bin/chromedriver')

        if not debug:
            atexit.register(self.close)
        # else:
        #     atexit.register(self.clear_download_dir)

    def close(self):
        self.clear_download_dir()
        with suppress(Exception):
            self.driver.close()
            time.sleep(1)
            self.driver.quit()

    def clear_download_dir(self):
        files = self.download_path.glob('*')
        for file in files:
            file.unlink()

    @staticmethod
    def random_wait():
        # metoda odczeka losowy czas od 5ms do 2 sekund, aby porawić niewykrywalność
        time.sleep(random.randrange(5, 90, 1) / 100)

    def wait_and_click(self, by, selector: str, timeout=20, js_click=False):
        # https://selenium-python.readthedocs.io/waits.html
        wait = WebDriverWait(self.driver, timeout)
        wait.until(EC.element_to_be_clickable((by, selector)))
        element = self.driver.find_element(by, selector)
        if js_click:
            self.driver.execute_script("arguments[0].click();", element)
        else:
            element.click()

        self.random_wait()  # just to be shure

    def screeshot_error(self):
        today = datetime.datetime.today().strftime('%Y-%m-%d')
        i = 1
        while True:
            path = self.errors_screenshots_path.joinpath(f'{today}-{i}.png')
            if not Path(path).is_file():
                break
            i += 1
        self.driver.save_screenshot(path)

    def screeshot_when_error(self, func, *args):
        try:
            func(*args)
        except Exception as e:
            print(type(e).__name__, e)
            self.screeshot_error()
            self.close()


def is_connection():
    with suppress(Exception):
        hostname = '1.1.1.1'
        host = socket.gethostbyname(hostname)
        s = socket.create_connection((host, 80), 2)
        s.close()
        return True

    return False


def connection_health():
    return sum([is_connection() for _ in range(20)]) / 20


def is_windows():
    return platform.system() == 'Windows'


if __name__ == '__main__':
    def func_with_error():
        return 3 + '3'

    def gut_func(*args, **kwargs):
        return f'value with deep message {args, kwargs}'

    class TestBot(Perfo, Selenium_bot):
        def __init__(self, log_prefix='TEST_BOT', debug=False):
            Perfo.__init__(self, log_prefix=log_prefix)
            self.log('start')
            Selenium_bot.__init__(self, debug=debug)

        def test(self):
            # self.driver.get('https://nowsecure.nl')
            self.driver.get('https://adamjuraczarnecki.github.io/headless-browser-test/')
            tests = self.driver.find_elements(By.CSS_SELECTOR, 'tbody tr')
            for test in tests:
                name, result = test.find_elements(By.TAG_NAME, 'td')
                self.log(f'{name.text:<25}: {result.text}')
            dupa += 1  # obvious error

    perfo = Perfo(log_to_console=True)
    # print(get_log_path(perfo.log_file_name))
    perfo.log('dupa8           dupa8')
    perfo.log('bardziej wyrafinowana forma komuikacji zawierająca wyrazy, wiele wyrazów a nawet zdania wielokrotnie złożone.')
    perfo.try_x_times(3, func_with_error)
    message = perfo.try_x_times(2, gut_func, True, max_retryes=1)
    perfo.log(message)
    perfo.log(f'is windows: {is_windows()}')
    print(connection_health())
    bot = TestBot()
    bot.try_x_times(1, bot.test)
    # perfo.send_mail_with_log()
