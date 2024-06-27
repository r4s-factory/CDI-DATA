from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager

class Paths:
    def __init__(self):
        try:
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        except:
            driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))
        self.driver = driver

"""d = Paths().driver
d.get("https://pythonbasics.org/selenium-firefox/")
input()
"""