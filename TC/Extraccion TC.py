import requests

from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import urllib
import pyodbc



url="https://www.carburando.com/notas/todos-los-campeones-del-turismo-carretera"
header={"User-Agent":"Mozilla/5.0"}

r=requests.get(url,headers=header)
r.raise_for_status()

soup=BeautifulSoup(r.text, "html.parser")


target=None
for h in soup.find_all(["h2"]):
    if "Listado de campeones del Turismo Carretera" in h.get_text():
        target=h
        break

   
if target is None:
    raise ValueError("No se encontró el encacabezado en la página")

table=target.find_next("table")


df = pd.read_html(str(table))[0]


server = 'DESKTOP-TFIGVI9\\SQLEXPRESS'    
user = 'usuario'
password = '1234'     # o usa trusted_connection
db_name = 'TurismoCarretera'

# Armar el string de conexión
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={db_name};"
    f"UID={user};"
    f"PWD={password};"
)

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};UID={user};PWD={password};DATABASE=master"
)
cn = pyodbc.connect(conn_str, autocommit=True)
cursor = cn.cursor()

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


sql = f"""
IF DB_ID('{db_name}') IS NULL
BEGIN
    CREATE DATABASE [{db_name}];
END
"""
cursor.execute(sql)
cursor.close()
cn.close()
print("Proceso terminado: base creada (si no existía).")

df.to_sql("TurismoCarretera",engine, if_exists="replace", index=False)

