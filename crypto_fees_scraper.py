import pip
from bs4 import BeautifulSoup
import requests
import pandas as pd
# File Management
import os
import sys

import boto3
import configparser


# Download missing packages
def install(package):
    if hasattr(pip, 'main'):
        pip.main(['install', package])
    else:
        pip._internal.main(['install', package])


# Function for creating new folders
def create_folder(new_path):
    folder_name = os.path.basename(new_path)

    # Checks if the folder already exists
    if not os.path.exists(new_path):
        print('{} folder created successfully.'.format(folder_name))
        os.mkdir(new_path)


# Main code


# Creates a log fileOn
log = open("scraper_logs.log", "a+", encoding="utf-8")
sys.stdout = log

# Uses the current directory as base directory
today = pd.to_datetime("today").strftime("%Y-%m-%d")
base_path = os.getcwd()
output_folder = 'output'
output_file = "crypto_fees_website_{}.csv".format(today)
html_path = r'https://cryptofees.info'

# Loop for creating folder for yahoo_stock_history and yahoo_id
for folder in [output_folder]:
    new_path = base_path + '\\' + folder
    create_folder(new_path)

# Get HTML using user agent
table = []
req = requests.get(html_path).text
# html = urlopen(req).read()

# table into a dataframe
soup = BeautifulSoup(req, "html.parser")

# Parse dataframe
for div in soup:
    td = soup.find_all('a')
    row = [div.text for div in td][6:-8]
    df = pd.DataFrame(row, columns=['Ticker'])
    df = df.Ticker.str.split("$", expand=True)
    df2 = df.set_axis(['Name', '1 Day Fees', '7 Day Fees'], axis=1)
    df3 = df2.replace(',', '', regex=True)
    df3['Name'].astype('str')
    # df3 = df2[['1 Day Fees', '7 Day Fees']].replacSwish and normal mode save(',','', regex=True)
    df3[['1 Day Fees', '7 Day Fees']].astype('float')

    df3['Date_Stamp'] = today
    # df = pd.read_csv(row, sep=",", index_col=0, header=None)

# file locally
df3.to_csv(base_path + '\\' + output_folder + '\\' + output_file, index=False, header=True)

# move file to archives

config_file = r"C:\Users\charl\Google Drive\2021\investing_programs\config.txt"

parser = configparser.ConfigParser()
parser.read(config_file)

print(parser.get("AWS", "aws_access_key_id"))

s3 = boto3.resource(
    service_name='s3',
    region_name=parser.get("AWS", "region"),
    aws_access_key_id=parser.get("AWS", "aws_access_key_id"),
    aws_secret_access_key=parser.get("AWS", "aws_secret_access_key")
)
# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)

s3.Bucket('1datadirectory').upload_file(Filename=base_path + '\\' + output_folder + '\\' + output_file,
                                        Key='crypto_fees_website/Raw/' + output_file)
sys.exit()
