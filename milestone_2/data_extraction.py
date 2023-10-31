from sqlalchemy import create_engine, MetaData, Table, select
from database_utils import DatabaseConnector
import pandas as pd
import requests
import tabula
import yaml

class DataExtractor(DatabaseConnector):
    """
    @desc : This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
    
    The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.
    """
    def __init__(self):
        # instantiate the Parent class DatabaseConnector
        super.__init__(self)
    
    def read_rds_table(super, table_name='legacy_users'):
        """
        @desc:  extract the database table to a pandas DataFrame.
        @inputs: 
            [1] table_name : the table that needs to be inspected
        """
        sql_engine = super.init_db_engine()
        sql_connection = sql_engine.connect()
        metadata = MetaData().reflect(sql_engine)
        users_table = Table(table_name, metadata, autoload=True, autoload_with=sql_engine)
    
        return pd.DataFrame(sql_connection.execute(select(users_table)).fetchall())
    
    def retrieve_pdf_data(self, link2pdf : str):
        """
        @desc: given the link to the pdf document, this function will return the pd.Dataframe of that doc
        """
        return pd.concat(tabula.read_pdf(link2pdf, stream=True, pages='all'), ignore_index=True)
    
    def list_number_of_stores(self, header : dict, endpoint : str = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"):
        """
        @desc: retrieves the store numbers data from the stores API, based on the input header and endpoint
        """
        response = requests.get(endpoint, headers=header)

        if response.status_code == 200:
            # Access the response data as JSON
            data = response.json()

            # Extract and print the name of the Pokémon
            num_stores = data['number_stores']
            print(f"Number of Stores: {num_stores}")
            return num_stores

        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")


    def retrieve_stores_data(self, retrieve_store_endpoint : str = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"):
        """
        @desc: given the retrieve a store endpoint, this function will return the data from all the stores as a pd.Dateframe
        """
        # the total stores information and a list to hold all of the responses
        store_number = 450
        store_detail = []
        
        # Read the YAML file and store its contents in a Python data structure (dictionary)
        yaml_file_path = '../api_key.yaml'
        with open(yaml_file_path, 'r') as file:
            yaml_data = yaml.safe_load(file)
        
        # setup the API key header
        headers = {
            "X-API-KEY": yaml_data["API_KEY"]
        }

        # loop through the store numbers and store the detail of the individual stores
        for i in range(store_number):
            url = retrieve_store_endpoint + str(i)
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                store_detail.append(response.json())

            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response Text: {response.text}")
        assert len(store_detail) == 450
        return pd.DataFrame(store_detail)