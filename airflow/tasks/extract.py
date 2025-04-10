import os
import json
import pandas as pd
from sqlalchemy import create_engine

def extract_data(path, table_name, base_dir=None):


    if base_dir is None:
        base_dir = os.getcwd()

    with open(os.path.join(base_dir, "credentials.json"), "r", encoding="utf-8") as file:
        credentials = json.load(file)

    db_host = credentials["db_host"]
    db_name = credentials["db_name"]
    db_user = credentials["db_user"]
    db_password = credentials["db_password"]

    csv_path = os.path.join(base_dir, path)
    grammys_data = pd.read_csv(csv_path, sep=",")

    pg_engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}?client_encoding=utf8")

    grammys_data.to_sql(table_name, pg_engine, if_exists="replace", index=False)
