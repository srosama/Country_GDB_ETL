from bs4 import BeautifulSoup
import pandas as pd
import requests
import sqlite3
import numpy as np
from datetime import datetime

# Code for ETL operations on Country-GDP data
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country","GDP_USD_millions"]
db_name    = "World_Economies.db"
table_name = "Countries_by_GDP"
csv_path   = "Countries_by_GDP.csv"

class etl:
    def __init__(self) -> None:
        self.df = pd.DataFrame()

    def extract(self, url, table_attribs):
        page = requests.get(url).text
        data = BeautifulSoup(page,'html.parser')
        df = pd.DataFrame(columns=table_attribs)
        tables = data.find_all('tbody')
        rows = tables[2].find_all('tr')
        for row in rows:
            col = row.find_all('td')
            if len(col)!=0:
                if col[0].find('a') is not None and '—' not in col[2]:
                    data_dict = {"Country": col[0].a.contents[0],
                                "GDP_USD_millions": col[2].contents[0]}
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df,df1], ignore_index=True)
        self.df = df

    def transform(self):
        ''' This function converts the GDP information from Currency
        format to float value, transforms the information of GDP from
        USD (Millions) to USD (Billions) rounding to 2 decimal places.
        The function returns the transformed dataframe.'''
        
        GDP_list = self.df["GDP_USD_millions"].tolist()  # Extracting GDP values from the DataFrame
        GDP_list = [float("".join(x.split(','))) for x in GDP_list]  # Cleaning and converting GDP values to float
        GDP_list = [np.round(x/1000,2) for x in GDP_list]  # Converting GDP values from millions to billions
        self.df["GDP_USD_billions"] = GDP_list  # Updating the GDP column in the DataFrame
        self.df.drop(columns=["GDP_USD_millions"], inplace=True)  # Drop the old GDP column
        return self.df
    
    def load_to_db(self, sql_connection, table_name):
        self.df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

    def run_query(self, query_statement, sql_connection):
        print(query_statement)
        query_output = pd.read_sql(query_statement, sql_connection)
        print(query_output)

    def load_to_csv(self,csv_path):
        self.df.to_csv(csv_path)

    def log_progress(self, message): 
        timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
        now = datetime.now() # get current timestamp 
        timestamp = now.strftime(timestamp_format) 
        with open("./etl_project_log.txt","a") as f: 
            f.write(timestamp + ' : ' + message + '\n')

if __name__ == '__main__':
    data = etl()
    data.extract(url, table_attribs)
    data.transform()
    data.load_to_csv(csv_path)
    data.log_progress('Data saved to CSV file')
    sql_connection = sqlite3.connect('World_Economies.db')
    data.log_progress('SQL Connection initiated.')
    data.load_to_db(sql_connection, table_name)
    data.log_progress('Data loaded to Database as table. Running the query')
    query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
    data.run_query(query_statement, sql_connection)
    data.log_progress('Process Complete.')
    sql_connection.close()
