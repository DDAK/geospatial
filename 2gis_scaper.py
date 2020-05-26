from multiprocessing.pool import ThreadPool
from bs4 import BeautifulSoup
from selenium import webdriver
import re
import csv

class ReusablePool:
    """
    Manage Reusable selenium webdriver objects for use by  Client objects.
    """

    def __init__(self, size):
        self._reusables = [get_driver() for _ in range(size)]

    def acquire(self):
        return self._reusables.pop()

    def release(self, reusable):
        self._reusables.append(reusable)

reg = r"[0-9]+.[0-9]+,[0-9]+.[0-9]+"

rg = re.compile(reg,re.IGNORECASE)
fileName = 'output_2gis_abu_auh1.csv'
def reg_get(test_str):
    matches = rg.finditer(test_str)
    for matchNum, match in enumerate(matches, start=1):
        # print ("{match}".format(match = match.group()))
        return match.group()
    return

def write_to_file(row, filename):
    with open(filename, 'a') as csvfile:
        fieldnames = ['loc', 'building','description', 'main_name']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerow(row)

def get_links(locs):
  url = f'https://2gis.ae/dubai/search/'
  urls = [url+l for l in locs]
  return urls

# threadLocal = threading.local()

def get_driver():
  # driver = getattr(threadLocal, 'driver', None)
  # if driver is None:
    chromeOptions = webdriver.ChromeOptions()

    chromeOptions.add_argument("--headless")
    driver = webdriver.Chrome('/chromedriver',chrome_options=chromeOptions)
    # setattr(threadLocal, 'driver', driver)
    return driver


def get_title(url):
    driver = reusable_pool.acquire()
    driver.get(url)
    content = driver.page_source
    description = None
    main_name = None
    building = None
    soup = BeautifulSoup(content, features='html.parser')

    if soup.find("a", href=True, attrs={'class': "_13ptbeu"}).text == "Some place":
        print('bad_url: ', url)
    else:
        href = soup.find('div', attrs={'class': "_y3rccd"})
        if soup.find('span', attrs={'class': '_oqoid'}) != None:
            building = soup.find('span', attrs={'class': '_oqoid'}).text
        if soup.find('div', attrs={'class': '_1p8iqzw'}) != None:
            description = [a.text for a in soup.findAll("span", attrs={'class': "_14quei"})]
        if soup.find("div", attrs={'class': "_18ijp46"})!= None:
            name = soup.find("div", attrs={'class': "_18ijp46"}).text
            main_name = name.split(',')

        write_to_file({
          'loc': reg_get(url).split(','),
          'building': building,
          "description": description,
          'main_name': main_name,
        },fileName)
    reusable_pool.release(driver)


if __name__ == '__main__':
    pool_size = 5
    reusable_pool = ReusablePool(pool_size)

    import pandas as pd
    df = pd.read_csv('data/abu_auh1.csv', error_bad_lines=False)
    lats = df['loc'].tolist()

    # https://stackoverflow.com/a/46049195
    """
    The rule of thumb is:
        1. IO bound jobs -> multiprocessing.pool.ThreadPool
        2. CPU bound jobs -> multiprocessing.Pool
        3. Hybrid jobs -> depends on the workload, I usually prefer the 'multiprocessing.Pool' due to the advantage 
        process isolation brings
    """
    ThreadPool(pool_size).map(get_title,get_links(lats))
