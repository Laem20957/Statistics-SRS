import os
import shutil
import sys
import time
import warnings
from urllib.parse import quote_plus

import pandas as pd
from python3.scripts.variables import get_value
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service
from sqlalchemy import create_engine

sys.path.append(r"T:\alidi_venv\vs code")

crm_link = r"https://crm.nestle.ru/main.aspx#256371554"
crm_login = get_value("crm_login")
crm_password = get_value("crm_password")
db_login = get_value("base_login")
db_password = get_value("base_password")
proxy = get_value("proxy_server")

useragent = r"useragent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1793.0"
path_to_driver = r"C:\Program Files (x86)\Microsoft\Edge Dev\Application\msedgedriver.exe"
download_path = r"C:\Users\kotelnikov.dy\Downloads\Ирина Тест.xlsx"
ide_path = r"T:\alidi_venv\vs code\python3\excel\Ирина Тест.xlsx"


def parse_website(path):
    service = Service(executable_path=path_to_driver)
    options = webdriver.EdgeOptions()
    options.add_argument(f"--proxy-server={proxy}")
    options.add_argument("--no-sandbox")
    options.add_argument(useragent)
    driver = webdriver.Edge(service=service, options=options)
    driver.get(crm_link)
    time.sleep(5)
    driver.find_element(By.XPATH, "//div[@id='bySelection']/div/div").click()  # isacloud
    time.sleep(5)
    input_login = driver.find_element(By.ID, "userNameInput")  # enter login
    input_login.clear()
    input_login.send_keys(crm_login)
    input_password = driver.find_element(By.ID, "passwordInput")  # enter password
    input_password.clear()
    input_password.send_keys(crm_password)
    driver.find_element(By.ID, "submitButton").click()  # submit
    time.sleep(5)
    driver.find_element(By.XPATH, "//span[@id='TabSFA']").click()  # dropdown
    time.sleep(5)
    driver.find_element(By.XPATH, "//a[@id='DICT']").click()  # template
    time.sleep(5)
    driver.find_element(By.XPATH, "//a[@id='cdc_territory']").click()  # territory
    time.sleep(5)
    iframe = driver.find_element(By.XPATH, "//iframe[@id='contentIFrame0' or @name='contentIFrame0']")  # iframe
    driver.switch_to.frame(iframe)
    driver.find_element(By.XPATH, "//a[@id='crmGrid_SavedNewQuerySelector']").click()  # dropdown_1
    time.sleep(5)
    driver.find_element(By.XPATH, "//span[@title='Ирина Тест']").click()  # user_template
    time.sleep(5)
    driver.switch_to.default_content()
    driver.find_element(By.XPATH, "//li[@command='cdc_territory|NoRelationship|HomePageGrid|Mscrm.ExportToExcel']").click()  # to_excel
    time.sleep(10)
    if os.path.isfile(download_path):
        shutil.move(download_path, ide_path)
    else:
        os.remove(download_path)
    driver.close()
    driver.quit()


def read_dataframe(path):
    warnings.simplefilter("ignore")
    dataframe = pd.read_excel(ide_path, engine="openpyxl")
    dataframe = dataframe.drop([
        "(Не изменять) Территория",
        "(Не изменять) Контрольная сумма строки",
        "GUID",
        "Мобильный сотрудник",
        "Регион"
    ], axis=1)
    dataframe = dataframe.rename(
        columns={
            "(Не изменять) Дата изменения": "Date Edit",
            "Уникальный идентификатор (Мобильный сотрудник) (Мобильный сотрудник)": "ID Merch",
            "Наименование": "Merch",
            "Родительская территория": "TSM",
            "Permanent merchandiser": "Type Merch",
            "Виртуальный": "Subtype Merch",
            "Площадка": "Depot",
            "Статус": "Status"
        })
    dataframe = dataframe.astype({"ID Merch": "str"})
    dataframe = dataframe.loc[(dataframe["Бизнес (ISA)"] == "G&C") & (dataframe["Канал продаж"] == "Мерчандайзер")]. \
        drop(["Бизнес (ISA)", "Канал продаж"], axis=1)
    return dataframe


def get_engine(server, db, username, password):
    conn = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+server+";DATABASE="+db+";UID="+username+";PWD="+password+";Trusted_Connection=yes"
    quoted = quote_plus(conn)
    new_con = "mssql+pyodbc:///?odbc_connect={}".format(quoted)
    engine = create_engine(new_con, fast_executemany=True, isolation_level="AUTOCOMMIT")
    return engine


def transfer_to_SQL(engine, df, sch, tablename):
    df.to_sql(tablename, engine, schema=sch, index=False, if_exists="replace", chunksize=None)


def main():
    global get_engine
    parse_website(crm_link)
    dataframe = read_dataframe(ide_path)
    get_engine = get_engine("NNSQL128", "DB_UTZ", db_login, db_password)
    transfer_to_SQL(get_engine, dataframe, "dbo", "NestleHierarchyTemplate")


main()
