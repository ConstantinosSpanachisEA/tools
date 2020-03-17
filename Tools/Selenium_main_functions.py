from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
import os
import time


class ToolsSelenium(object):
    def __init__(self, downloads_path):
        try:
            current_path = os.getcwd()
            files = os.listdir(current_path)
            if "chromedriver.exe" in files:
                print("Chrome Driver is inside the current working directory")
            else:
                print("Please download Chromedriver.exe and add it to the path")
        except Exception as e:
            print(e)
        self.chrome_options = webdriver.ChromeOptions()
        try:
            if os.path.exists(downloads_path):
                print(f"Changing the downloading path to {downloads_path}")
            else:
                print("Creating Directory")
                os.mkdir(downloads_path)
        except Exception as e:
            print(e)
        prefs = {"download.default_directory": downloads_path}
        self.chrome_options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome()
        self.element = None

    def navigate_to_link(self, url):
        self.driver.get(url=url)
        time.sleep(2)

    def find_element_by_xpath(self, xpath, return_element=False):
        j = 1
        while j < 10:
            try:
                if self.driver.find_element_by_xpath(xpath=xpath):
                    self.element = self.driver.find_element_by_xpath(xpath=xpath)
                    j = 10
                    if return_element:
                        return self.element
            except NoSuchElementException:
                print('Element not found')
                j += 1

    def find_element_by_text(self, text, return_element=False):
        j = 1
        while j < 10:
            try:
                if self.driver.find_element_by_link_text(link_text=text):
                    self.element = self.driver.find_element_by_link_text(link_text=text)
                    j = 10
                    if return_element:
                        return self.element
            except NoSuchElementException:
                print('Element not found')
                j += 1

    def click_element(self, element=None):
        if element is None:
            self.element.clck()
        else:
            element.click()


if __name__ == '__main__':
    selenium_tool = ToolsSelenium(os.path.join(os.getcwd(), "Downloads"))
    selenium_tool.navigate_to_link(r"http://utg.ua/wp-content/uploads/cdd/ARCHIVE/UGS/EN/")
    element = selenium_tool.find_element_by_xpath("/html/body/table/tbody/tr[4]/td[2]/a", return_element=True)
    print(element)
