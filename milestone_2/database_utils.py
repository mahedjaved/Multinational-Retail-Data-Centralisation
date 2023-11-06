"""
@reqs:
    [1] PyYAML : !pip install PyYAML
    [2] psycopg : !pip install psycopg2
"""
from sqlalchemy import create_engine, MetaData
import pandas as pd
import yaml

class DatabaseConnector:
    """
    @desc : use to connect with and upload data to the database.
    """
    def __init__(self):
        pass

    
    def read_db_creds(self):
        """
        @desc: reqs-> install PyYAML: pip install PyYAML
        """
        yaml_file_path = '../db_creds.yaml'

        # Read the YAML file and store its contents in a Python data structure (dictionary)
        with open(yaml_file_path, 'r') as file:
            return yaml.safe_load(file)
        

    def init_db_engine(self):
        yaml_data = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = yaml_data['RDS_HOST']
        USER = yaml_data['RDS_USER']
        PASSWORD = yaml_data['RDS_PASSWORD']
        PORT = yaml_data['RDS_PORT']
        DATABASE = yaml_data['RDS_DATABASE']

        return create_engine(f"{DATABASE_TYPE}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
    
    def list_db_tables(self):
        # setup sql engine and connect
        sql_engine = self.init_db_engine()
        sql_engine.connect()

        # metadata, holds collection of table info, their data types, names 
        metadata = MetaData()
        metadata.reflect(sql_engine)
        table_names = metadata.tables.keys()

        # return the table names in a list format
        return list(table_names)
    

    def upload_to_db(self, in_df : pd.DataFrame, table_name : str):
        # Read the YAML file and store its contents in a Python data structure (dictionary)
        yaml_file_path = '../local_db_creds.yaml'
        with open(yaml_file_path, 'r') as file:
            yaml_data = yaml.safe_load(file)
        
        # setup credentials
        DATABASE_TYPE = 'postgresql+psycopg2'
        ENDPOINT = yaml_data['LOCAL_HOST']
        USER = yaml_data['LOCAL_USER']
        PASSWORD = yaml_data['LOCAL_PASSWORD']
        PORT = yaml_data['LOCAL_PORT']
        DATABASE = yaml_data['LOCAL_DATABASE']

        # connect and upload, index=False iff there exists a seperate column for index
        engine = create_engine(f"{DATABASE_TYPE}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        connection = engine.connect()
        in_df.to_sql(table_name, engine, if_exists='replace', index=False)