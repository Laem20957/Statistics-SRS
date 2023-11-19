import json
import time
import urllib.parse
import urllib.request
from urllib.parse import quote_plus

import pandas as pd
from python3.scripts.variables import get_value
from sqlalchemy import Integer, String, create_engine, text

base_login = get_value("base_login")
base_password = get_value("base_password")
apikey = get_value("geocoderkey")


def get_engine(server, db, username, password):
    conn = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+server+";DATABASE="+db+";UID="+base_login+";PWD="+base_password+";Trusted_Connection=yes"
    quoted = quote_plus(conn)
    new_con = "mssql+pyodbc:///?odbc_connect={}".format(quoted)
    engine = create_engine(new_con, fast_executemany=True)
    return engine


server_src = get_engine("NNSQL135", "JDEOLAP", base_login, base_password)
server_destination = get_engine("SQL280", "DataLake", base_login, base_password)


def transfer_to_SQL(engine, df, sch, tablename):
    dtypes = {
        "id": Integer,
        "address": String,
        "coordinates": String,
        "type_object": String,
        "address_coord": String
    }
    df.to_sql(tablename, engine, schema=sch, index=False, if_exists="replace", dtype=dtypes, chunksize=None)


def get_coordinates(dataframe):
    geocoderkey = apikey
    request = "https://geocode-maps.yandex.ru/1.x/?apikey=%apikey%&format=json&geocode=%request%"
    address = dataframe.replace("&&", "&")
    req = request.replace("%apikey%", geocoderkey).replace("%request%", urllib.parse.quote(address))
    data = json.loads(urllib.request.urlopen(req).read())
    geoobj = data["response"]["GeoObjectCollection"]["featureMember"]

    for x in geoobj:
        if len(x["GeoObject"]["Point"]["pos"]) > 0:
            attribute_point = "Y"
            break
    k = ""
    if attribute_point == "Y":
        for x in geoobj:
            k = x["GeoObject"]["Point"]["pos"]
            type_object = x["GeoObject"]["metaDataProperty"]["GeocoderMetaData"]["kind"]
            address_coord = x["GeoObject"]["metaDataProperty"]["GeocoderMetaData"]["text"]
            break
    coord = k
    return coord, type_object, address_coord


def get_dataframe():
    with open(r"T:\alidi_venv\vs code\python3\sql\script.sql", "r", encoding="utf-8") as file:
        sql = file.read()
    with server_src.connect() as conn:
        result = conn.execute(text(sql))
        rows = result.fetchall()
    dataframe = pd.DataFrame(rows, columns=result.keys())

    i = 0
    result_geocoder = []
    for row in dataframe["address"]:
        result_geocoder.append(get_coordinates(row))
        if i % 100 == 0:
            time.sleep(5)
        i = i + 1
    dataframe[["coordinates", "type_object", "address_coord"]] = result_geocoder
    return dataframe


def main():
    dataframe = get_dataframe()
    transfer_to_SQL(server_destination, dataframe, "analysis", "result_geocoder")


main()
