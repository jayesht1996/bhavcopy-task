import urllib.request
import zipfile
import pandas as pd
import redis
from bs4 import BeautifulSoup
import requests
import os

BHAVCOPY_URL = "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx"

def redis_conn():
    ''' Creating redis connection using StrictRedis '''
    
    return redis.StrictRedis(host="localhost", 
                            db=1, 
                            charset='utf-8', 
                            decode_responses=True)

def download_bhavcopy():
    ''' Using bhavcopy url get latest equity link
    use BeautifulSoup to parse html and download latest zip file
    '''

    html = requests.get(BHAVCOPY_URL)
    soup = BeautifulSoup(html.text, "html.parser")
    equity_tag = soup.find(id='ContentPlaceHolder1_btnhylZip')
    equity_link = equity_tag.get('href', None)
    filepath = os.path.join(os.getcwd(), 
                            "exportapp",
                            "bhavcopy_data", 
                            "bhavcopy.zip")
    urllib.request.urlretrieve(equity_link, filepath)


def extract_bhavcopy_zip():
    ''' Using ZipFile extract all the files from zip '''

    bhavcopy_dir = os.path.join(os.getcwd(), 
                            "exportapp",
                            "bhavcopy_data")

    bhavcopy_zip = os.path.join(bhavcopy_dir, "bhavcopy.zip")

    zip_ref = zipfile.ZipFile(bhavcopy_zip, 'r')
    equity_filename = zip_ref.namelist()[0]
    zip_ref.extractall(bhavcopy_dir)
    zip_ref.close()
    return equity_filename


def store_bhavcopy_data(equity_filename):
    ''' Read csv file using pandas and store csv data 
    and store details on redis db
    '''
    conn = redis_conn()
    csv_data = pd.read_csv(os.path.join(os.getcwd(), 
                                        "exportapp",
                                        "bhavcopy_data", 
                                        equity_filename))
    csv_data = csv_data[['SC_CODE', 'SC_NAME', 'OPEN', 'HIGH', 'LOW', 'CLOSE']].copy()

    conn.flushall()
    for index, row in csv_data.iterrows():
        conn.hmset(row['SC_CODE'], row.to_dict())



def get_latest_equity():
    ''' Get latest 10 records from redis db '''

    results = []
    conn = redis_conn()
    keys = conn.keys('*')

    for equity in keys:        
        results.append(conn.hgetall(equity))

    return results[:10]
