import psycopg2
import os
import pandas as pd
from sqlalchemy import create_engine

def get_connection():
    conn = psycopg2.connect(
        dbname="statcast",
        user="postgres",
        password="jamin",
        host="localhost",
        port="5432"
    )
    return conn

def get_pbp_data(years):
    conn = get_connection()
    
    def query_year(year):
        query = f"""
            SELECT * FROM statcast_all
            WHERE game_year = {year};
        """
        df = pd.read_sql(query, conn)
        print(f"{year}: {len(df)} rows")
        return df
    
    pbp_data_list = [query_year(year) for year in years]
    pbp_data = pd.concat(pbp_data_list, ignore_index=True)
    
    return pbp_data

def get_swing_data(years):
    conn = get_connection()
    
    def query_year(year):
        query = f"""
            SELECT * FROM statcast_all
            WHERE swing_length IS NOT NULL
            AND bat_speed IS NOT NULL
            AND game_year = {year};
        """
        df = pd.read_sql(query, conn)
        print(f"{year}: {len(df)} rows")
        return df
    
    pbp_data_list = [query_year(year) for year in years]
    pbp_data = pd.concat(pbp_data_list, ignore_index=True)
    
    return pbp_data
def get_description_data(year):
    conn = get_connection()
    query = f"""
            SELECT * FROM statcast_all
            WHERE events IS NOT NULL
            AND game_year = {year};
            """
    return pd.read_sql(query,conn)

def upload_to_sql(data_frame, table_name):
     engine = create_engine('postgresql://postgres:jamin@localhost:5432/statcast')

     data_frame.to_sql(
          table_name,
          engine,
          if_exists='replace',
          index=False
     )
     print(f"DataFrame {data_frame} uploaded successfully to table: {table_name}")

def get_table(table_name, year=None):
    conn = get_connection()
    if year is None:
        query = f"""
        SELECT * FROM {table_name}
        """
    else: 
        query = f"""
                SELECT * FROM {table_name}
                WHERE game_year = {year};
                """
    table = pd.read_sql(query, conn)

    return table

