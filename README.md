# Multinational-Retail-Data-Centralisation

# Milestones

## Milestones 2 : Task Description

- Initialise a new database locally to store the extracted data.

- Set up a new database within pgadmin4 and name it sales_data.

- The database will store all the company information once we extract it for the various data sources.

## Milestones 2 : Code Description

- `data_extraction.py` : used to extract data from different data sources

- `database_utils.py` : used to connect with and upload data to the database

- `data_ckeaning.py` : used to clean data from each of the data sources

# Project Description

You work for a multinational company that sells various goods across the globe.

Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.

In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.

Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.

You will then query the database to get up-to-date metrics for the business.

# Some keynotes learned from this project

- When listing table names from SQL alchemy, it is more computationally to use the Inspector object as opposed to the MetaData class. The Metadata object holds collection of table info, their data types, schema names etc

- however MetaData() includes thread safety -> meaning it can handle concurrent tasks from multiple thread (computationally efficient when multiple threads need access to same resource) `Reference: ` obtained from here : https://docs.sqlalchemy.org/en/20/core/metadata.html

- A `FacadeDict` : basically it is like Python's default Dict() datastrucutre, SQLAlchemy claims it is more efficient for lookup and management. Since there is no resource that compares the two, the claim that `Facade` is better is clearly, in itself, a facade :)

- However, SQLAlchemy claims it to be publically immutable, hence it is probably more secure to use, `Reference: ` https://pydoc.dev/sqlalchemy/latest/sqlalchemy.util._collections.FacadeDict.html

# Software Requirements

- Pandas : !pip install pandas
- SQLAlchemy : !pip install SQLAlchemy
- Psycopg2 : !pip install psycopg2
- Tabula : !pip install tabula-py

# #TODO : for improving code (quality and performance)

- Re-run task 3 miletone 2 with Inspector object, use %%time magic func. to compare performance with MetaData object in SQLAlchemy

# Resources

- SQLAlchemy common commands : https://towardsdatascience.com/sqlalchemy-python-tutorial-79a577141a91
