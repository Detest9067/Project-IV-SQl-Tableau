# Project 4- SQL and Tableau
The aim of this project is to practice data extraction, trimming, and loading into an SQL database, using pymysql to perform queries and visualize the subsequent results on a Tableau Public dashboard.
### Required Libraries
* Pandas
* PyMySQL
* Requests
* OS
* SQLAlchemy
* Glob
* JSON
* Logging
* NLTK (including Vader Lexicon)

### Datasets used
* [Game of Thrones Script CSV](https://www.kaggle.com/datasets/albenft/game-of-thrones-script-all-seasons) 
* Game of Thrones [RapidAPI](https://rapidapi.com/mrmalinsky08-MHGsijNPHM/api/game-of-thrones1/)

### Data Manipulation
The code in the src.py file contains the functions necessary to prepare the data for the Tableau visualizations. For each season, I extracted the average polarity per character. Once that was established, I looked at the top 10 most positive/negative characters and exported them to a separate csv for visualization.

### Key points
* The data includes the top 10 most positive/negative characters
* Only Seasons 1-4 are included
* The Jupyter Notebook called 'ETL' contains all the called functions

### [Tableau Dashboard](https://public.tableau.com/app/profile/gi.diorno/viz/G_O_T_SIA/Pres_1#1)
