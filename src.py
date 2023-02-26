import pymysql
import sqlalchemy as alch
import pandas as pd
import os 
from dotenv import load_dotenv
import logging
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
nltk.download('vader_lexicon')

load_dotenv()
password = os.getenv("sql_pass")

db = 'GOT'
connection= f'mysql+pymysql://root:{password}@localhost/{db}'
engine = alch.create_engine(connection)


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

def analyze_sentiment(season):
    with engine.connect() as conn:
        query = f"SELECT name, sentence FROM {season};"
        df = pd.read_sql_query(query, conn)
        sia = SentimentIntensityAnalyzer()
        df['polarity_score'] = df['sentence'].apply(lambda x: sia.polarity_scores(x)['compound'])
        df.to_sql(season, conn, if_exists='replace', index=False)

def get_average_polarity(season):
    with engine.connect() as conn:
        query = f"SELECT name, sentence, polarity_score FROM {season};"
        df = pd.read_sql_query(query, conn)
    grouped = df.groupby("name").agg({"polarity_score": "mean"}).reset_index()
    return grouped

def top_characters(season):
    with engine.connect() as conn:
        query = f"SELECT name, AVG(polarity_score) as avg_polarity, COUNT(sentence) as num_sentences FROM {season} GROUP BY name HAVING num_sentences > 5 ORDER BY avg_polarity DESC LIMIT 10;"
        top_df = pd.read_sql_query(query, conn)
        top_df.to_csv(f"data/top_characters_{season}.csv", index=False)
        return top_df


def bottom_characters(season):
    with engine.connect() as conn:
        query = f"SELECT name, AVG(polarity_score) as avg_polarity, COUNT(sentence) as num_sentences FROM {season} GROUP BY name HAVING num_sentences > 5 ORDER BY avg_polarity ASC LIMIT 10;"
        bottom_df = pd.read_sql_query(query, conn)
        bottom_df.to_csv(f"data/bottom_characters_{season}.csv", index=False)
        return bottom_df