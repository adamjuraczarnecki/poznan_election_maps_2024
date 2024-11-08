from perfo import Perfo, Selenium_bot
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

komitety_mapa = {
    3: 'Trzecia Droga',
    4: 'Lewica',
    5: 'Koalicja Obywatelska',
    10: 'Społeczny Poznań',
    11: 'Zjednoczona Prawica',
    12: 'Konfederacja Propolska'
}
VOTES_SELECTOR = '#DataTables_Table_0_wrapper tbody tr'
BASE_URL = 'https://www.wybory.gov.pl/samorzad2024/pl/rada_gminy/okreg/306400/'
TARGET_TABLE = 'intel-tool.wybory2024.candidates'
table_schema = [
    {
        "name": "name",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "list_number",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "list_name",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "okreg_number",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "candidate_number",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "obwod",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "votes",
        "type": "INTEGER",
        "mode": "NULLABLE"
    }
]


class PKW_Bot(Perfo, Selenium_bot):

    def __init__(self, log_prefix='PKW-BOT', log_to_file=True, log_to_console=None, debug=False, log_file_name='erli_perfo_daily'):
        Perfo.__init__(self, log_prefix=log_prefix, log_to_console=log_to_console, log_to_file=log_to_file, log_file_name=log_file_name)
        self.log('start')
        Selenium_bot.__init__(self, debug=debug)

    def scrape_candidate(self, url):
        wait = WebDriverWait(self.driver, 240)
        self.driver.get(url)
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, VOTES_SELECTOR)))
        name = self.driver.find_element(By.CLASS_NAME, 'name').text
        info_text = self.driver.find_element(By.CSS_SELECTOR, '.candidate dl').text.split('\n')
        info_len = len(info_text)
        list_number = int(info_text[7 if info_len == 16 else 9])
        candidate_number = int(info_text[9 if info_len == 16 else 11])
        okreg_number = int(info_text[15 if info_len == 16 else 17].split(' ')[3])

        odwody = [x.find_elements(By.TAG_NAME, 'td') for x in self.driver.find_elements(By.CSS_SELECTOR, VOTES_SELECTOR)]

        data = [{
            'name': name,
            'list_number': list_number,
            'list_name': komitety_mapa[list_number],
            'okreg_number': okreg_number,
            'candidate_number': candidate_number,
            'obwod': int(x[0].text),
            'votes': int(x[3].text)
        } for x in odwody]

        return data

    def get_candidates_from_okreg(self, okreg_number):
        wait = WebDriverWait(self.driver, 240)
        self.driver.get(f'{BASE_URL}{okreg_number}')
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, VOTES_SELECTOR)))
        links = [
            x.get_attribute('href')
            for x in self.driver.find_elements(By.CSS_SELECTOR, 'td a')
            if '/samorzad2024/pl/rada_gminy/kandydat/' in x.get_attribute('href')
        ]

        return list(set(links))

    def scrape_okreg(self, okreg_number):
        data = []
        links = self.get_candidates_from_okreg(okreg_number)
        candidates = len(links)
        self.log(f'i have {candidates} links')
        for i, link in enumerate(links):
            data += self.scrape_candidate(link)
            self.random_wait()
            self.log(f'done {i+1}/{candidates}')

        return data

    def job(self):
        okregs = [1, 2, 3, 4, 5, 6]
        data = []
        for okreg in okregs:
            self.log(f'start scrapping okreg {okreg}')
            data = self.scrape_okreg(okreg)
            message = self.bq.load_to_bq_as_file(TARGET_TABLE, data, mode='append', schema=table_schema)
            self.log(message)

        self.log('i chuj')


if __name__ == '__main__':
    bot = PKW_Bot(debug=True)
    bot.job()
