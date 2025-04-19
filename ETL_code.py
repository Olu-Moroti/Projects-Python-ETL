import requests
import os
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# Get the data
URLs = ['https://github.com/annexare/Countries/blob/master/data/continents.json',
        'https://github.com/annexare/Countries/blob/master/data/countries.2to3.json',
        'https://github.com/annexare/Countries/blob/master/data/countries.3to2.json',
        'https://github.com/annexare/Countries/blob/master/data/countries.json',
        'https://github.com/annexare/Countries/blob/master/data/languages.json']

URLs = [i.replace('github.com', 'raw.githubusercontent.com').replace('/blob', '') for i in URLs]

print("Getting the json data...")
# Extract data
for url in URLs:
    filename = os.path.split(url)[1]
    response = requests.get(url)
    open(filename, 'wb').write(response.content)

print("Reading the json files...")
continents_df = pd.read_json('continents.json', orient='index')
countries_2to3_df = pd.read_json('countries.2to3.json', orient='index')
countries_3to2_df = pd.read_json('countries.3to2.json', orient='index')
countries_df = pd.read_json('countries.json').transpose()
languages_df = pd.read_json('languages.json').transpose()

continents_df = continents_df.reset_index(0)
countries_2to3_df = countries_2to3_df.reset_index(0)
countries_3to2_df = countries_3to2_df.reset_index(0)
countries_df = countries_df.reset_index(0)
languages_df = languages_df.reset_index(0)

print("Connecting to the database...")
# Login to database
# print("Login to the localhost database.")
dbname = 'postgres'
username = 'postgres'
password = 'admin'

engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/{dbname}')

# Load data to database
# try:
print("Creating and loading the data to the database...")
print("Loading CONTINENTS data...")
continents_df.to_sql('CONTINENTS', con=engine, if_exists='replace', index=False)
print("Loading COUNTRIES2TO3 data...")
countries_2to3_df.to_sql('COUNTRIES2TO3', con=engine, if_exists='replace', index=False)
print("Loading COUNTRIES3TO2 data...")
countries_3to2_df.to_sql('COUNTRIES3TO2', con=engine, if_exists='replace', index=False)
print("Loading COUNTRIES data...")
countries_df.to_sql('COUNTRIES', con=engine, if_exists='replace', index=False)
print("Loading LANGUAGES data...")
languages_df.to_sql('LANGUAGES', con=engine, if_exists='replace', index=False)
# except ValueError: # TABLE NAME is already in the database
#     pass
# else:
#     pass

query1 = """

SELECT 
    "CONTINENTS"."0" AS "Continent",
    COUNT(*) AS "Country Count"
FROM "COUNTRIES"
LEFT JOIN "CONTINENTS" ON "CONTINENTS".index="COUNTRIES".continent 
GROUP  BY 1

"""

query2 = """

DROP TABLE IF EXISTS language_array;
CREATE TABLE language_array AS (
WITH delete_wrong_char AS (
SELECT 
    name,
    rtrim(LTRIM(languages,'{'),'}') AS languages
FROM "COUNTRIES"
)
SELECT 
    name,
    STRING_TO_ARRAY(TRIM(languages),',') AS languages
FROM
    delete_wrong_char
);

WITH single_language_cte AS (
    SELECT 
        name,
        UNNEST(languages) AS each_language 
    FROM language_array
)
SELECT 
    "LANGUAGES".name AS "Language",
    STRING_AGG(single_language_cte.name,', ') AS "Countries"
FROM 
    single_language_cte
    LEFT JOIN "LANGUAGES" ON "LANGUAGES".index=single_language_cte.each_language
GROUP BY 1;


"""

query3 = """

DROP TABLE IF EXISTS language_array;
CREATE TABLE language_array AS (
WITH delete_wrong_char AS (
SELECT 
    name,
    rtrim(LTRIM(languages,'{'),'}') AS languages
FROM "COUNTRIES"
)
SELECT 
    name,
    STRING_TO_ARRAY(TRIM(languages),',') AS languages
FROM
    delete_wrong_char
);

SELECT 
    name AS "Country",
    COALESCE(ARRAY_LENGTH(languages,1),0) "Lng Count" 
FROM language_array;

"""

queries = [query1, query2, query3]

def  refresh_view(query):
    conn = psycopg2.connect(database=dbname, user=username, password=password)
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    conn.close()
    return result

print("Running the queries...")
if __name__ == '__main__':
    #view_names = ['view1','view2', 'view3']
    result = map(refresh_view, queries)
    result = [i for i in result]
    result1 = result[0]
    result2 = result[1]
    result3 = result[2]

    print("\n\nTask 1")
    print("(Continent, Country Count)")
    print("=============================================================")
    for row in result1:
        print(row)

    print("\n\nTask 2")
    print("(Language, Countries)")
    print("=============================================================")
    for row in result2:
        print(row)

    print("\n\nTask 3")
    print("(Country, Lng Count)")
    print("=============================================================")
    for row in result3:
        print(row)

print("\n\nCleaning up...")
for file in os.listdir('.'):
    if file.endswith('.json'):
        os.remove(file)

print("Done.")
