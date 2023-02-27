import pymysql
import sqlalchemy as alch
import pandas as pd
import glob
import os 
from dotenv import load_dotenv
import requests
import json
import logging
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
nltk.download('vader_lexicon')

load_dotenv()
password = os.getenv("sql_pass")

#creating db and loading in the csv 
def load_csv_to_mysql(db):
    password = os.environ.get('sql_pass')
    
    connection = f'mysql+pymysql://root:{password}@localhost/'
    
    engine = alch.create_engine(connection)
    
    engine.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
    connection = f'mysql+pymysql://root:{password}@localhost/{db}'
    engine = alch.create_engine(connection)
    
    df = pd.read_csv('script/Game_of_Thrones_Script.csv')
    df.to_sql(name='script', con=engine, if_exists='replace', index=False)

db = 'Game_of_Thrones'
connection= f'mysql+pymysql://root:{password}@localhost/{db}'
engine = alch.create_engine(connection)

#loading a table for each season with name and sentence extracted 
def create_season_table(season_number):
    query = f"""
        CREATE TABLE season_{season_number} AS 
        SELECT name, sentence 
        FROM GOT.Game_of_Thrones_Script where Season like 'Season {season_number}';"""
    try:
        with engine.begin() as conn:
            conn.execute(query)
        logging.info(f"Table for Season {season_number} created successfully!")
    except Exception as e:
        logging.error(f"Error creating table for Season {season_number}: {e}")

#looking at sentiments per character 
def analyze_sentiment(season):
    with engine.connect() as conn:
        query = f"SELECT name, sentence FROM {season};"
        df = pd.read_sql_query(query, conn)
        sia = SentimentIntensityAnalyzer()
        df['polarity_score'] = df['sentence'].apply(lambda x: sia.polarity_scores(x)['compound'])
        df.to_sql(season, conn, if_exists='replace', index=False)

#looking at the average polarity per character
def get_average_polarity(season):
    with engine.connect() as conn:
        query = f"SELECT name, sentence, polarity_score FROM {season};"
        df = pd.read_sql_query(query, conn)
    grouped = df.groupby("name").agg({"polarity_score": "mean"}).reset_index()
    return grouped

#extracting top chacters with more than 5 lines
def top_characters(season):
    with engine.connect() as conn:
        query = f"SELECT '{season}' as season, name, AVG(polarity_score) as avg_polarity, COUNT(sentence) as num_sentences FROM {season} GROUP BY name HAVING num_sentences > 5 ORDER BY avg_polarity DESC LIMIT 10;"
        top_df = pd.read_sql_query(query, conn)
        top_df.to_csv(f"data/top_characters_{season}.csv", index=False)
        return top_df

#extracting the negative characters with more than 5 lines
def bottom_characters(season):
    with engine.connect() as conn:
        query = f"SELECT '{season}' as season, name, AVG(polarity_score) as avg_polarity, COUNT(sentence) as num_sentences FROM {season} GROUP BY name HAVING num_sentences > 5 ORDER BY avg_polarity ASC LIMIT 10;"
        bottom_df = pd.read_sql_query(query, conn)
        bottom_df.to_csv(f"data/bottom_characters_{season}.csv", index=False)
        return bottom_df

#combining all top/bottom characters into one csv for easy visualizing
def combine_csvs(folder_path, output_file, exclude_file=None):
    
    csv_files = glob.glob(f'{folder_path}/*.csv')
    
    dataframes = []
    for file in csv_files:
        
        df = pd.read_csv(file)
        dataframes.append(df)

    combined_df = pd.concat(dataframes)
    combined_df.to_csv(output_file, index=False)

#extracting a csv for each significant house
# def extract_house(house_name):
    
#     with engine.connect() as conn:
#         query = f"SELECT * FROM script WHERE Name LIKE '%%{house_name}%%'"
#         df = pd.read_sql(query, conn)
#         sia = SentimentIntensityAnalyzer()
#         df['polarity_score'] = df['Sentence'].apply(lambda x: sia.polarity_scores(x)['compound'])
#         df = df[['Name', 'polarity_score']]
#         df = df.groupby(['Name']).mean()
#         table_name = house_name.lower()
#         df.to_csv(f"house/{house_name}.csv", index=True)
#         df.to_sql(table_name, conn, if_exists='replace', dtype={'Name': alch.types.String(255)})
def extract_house(house_name):
    with engine.connect() as conn:
        query = f"SELECT * FROM script WHERE Name LIKE '%%{house_name}%%'"
        df = pd.read_sql(query, conn)
        sia = SentimentIntensityAnalyzer()
        df['polarity_score'] = df['Sentence'].apply(lambda x: sia.polarity_scores(x)['compound'])
        df['house_name'] = house_name
        df = df[['house_name', 'Name', 'polarity_score']]
        df = df.groupby(['house_name', 'Name']).mean()
        table_name = house_name.lower()
        df.to_csv(f"house/{house_name}.csv", index=True)
        df.to_sql(table_name, conn, if_exists='replace', dtype={'Name': alch.types.String(255), 'house_name': alch.types.String(255)})

        


#game of thrones API to retrieve pictures
def got_api():
    url = "https://game-of-thrones1.p.rapidapi.com/Characters"

    headers = {
        "X-RapidAPI-Key": os.environ.get('RAPIDAPI_KEY'),
        "X-RapidAPI-Host": os.environ.get('RAPIDAPI_HOST')
    }
    params = {
    "fields": "firstName,lastName,imageUrl"
    }   
    response = requests.request("GET", url, headers=headers)

    if response.status_code == 200:
        data = json.loads(response.text)
        with open('images/game_of_thrones_characters.json', 'w') as f:
            json.dump(data, f)
        return data
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None
