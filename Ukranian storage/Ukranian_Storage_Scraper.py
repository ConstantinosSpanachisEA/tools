import selenium
import numpy as np
import os
import pandas as pd
import datetime
import time
from Tools.Selenium_main_functions import ToolsSelenium

url = r'http://utg.ua/wp-content/uploads/cdd/ARCHIVE/UGS/EN/'
selenium_tool = ToolsSelenium(downloads_path=os.path.join(os.getcwd(), "Downloads"))
selenium_tool.navigate_to_link(url=url)
current_date = (datetime.date.today()-datetime.timedelta(days=2)).strftime("%d.%m")
text_for_element = f"AllUGS_UTG_en_{current_date}...>"
element = selenium_tool.find_element_by_text(text_for_element, return_element=True)
element.click()






