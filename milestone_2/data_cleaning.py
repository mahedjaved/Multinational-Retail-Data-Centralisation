import pandas as pd
import re
from dateutil.parser import parse
class DataCleaning:
    """
    @desc : methods to clean data from each of the data sources
    """
    
    # ================================================================== #
    #                   UTILITY FUNCTIONS
    # ================================================================== #
    @staticmethod
    def is_alpha(in_str):
        """
        @desc: function to check if the column has alphabets entries
        """
        return any(c.isalpha() for c in in_str)
    
    @staticmethod
    def is_alphanumeric(in_str):
        """
        @desc: function to check if the column has alphanumeric entries
        """
        return bool(re.fullmatch(r'^[a-zA-Z0-9_]*$', in_str))

    @staticmethod
    def has_yyyy_mm_dd_format(in_str):
        """
        @desc: function to decide if the a column of a data has date format yyyy-mm-dd
        """
        return bool(re.fullmatch(r'\d{4}-\d{2}-\d{2}', in_str))

    @staticmethod
    def convert_date_to_yyyy_mm_dd(in_column : pd.core.series.Series):
        """
        @desc: function to set the date column with date format yyyy-mm-dd
        """
        in_column = in_column.apply(parse)
        in_column = pd.to_datetime(in_column, infer_datetime_format=True, errors='coerce')
        
        return in_column

    # ================================================================== #
    #                   PRIMARY FUNCTIONS
    # ================================================================== #
    def clean_user_data(self, users_df : pd.DataFrame, table_name : str = 'legacy_users'):
        """
        @desc: pre-process the legacy users table
        """
        if users_df.isnull().sum().sum() and users_df.isna().sum().sum():
            raise f"The database : {table_name}, has total {users_df.isnull().sum().sum()} NULL values and {users_df.isna().sum().sum()} NaN values"
        else:
            print(f"[usrmsg] No NULLs or NaNs found in {table_name}")
        users_df_processed = users_df[~users_df.apply(lambda row: row.astype(str).str.contains('NULL').any(), axis=1)]
        # check for data types 
        #   -1) always begin with dropping duplicates and storing as a seperate file
        users_df_processed = users_df_processed.drop_duplicates()
        #   -2) set all columns except index to be of string format
        str_convert_dict = {col: 'string' for col in users_df_processed.columns if col not in ['index']}
        users_df_processed = users_df_processed.astype(str_convert_dict)
        #   -3) remove all entries that are pure alphanumeric
        users_df_processed = users_df_processed[~users_df_processed['email_address'].apply(is_alphanumeric)]
        #   -4) DoB and join_date should be datetime format and of type yyyy-mm-dd
        users_df_processed['date_of_birth'] = convert_date_to_yyyy_mm_dd(users_df_processed['date_of_birth'])
        users_df_processed['join_date'] = convert_date_to_yyyy_mm_dd(users_df_processed['join_date'])
        #   -5) convert all 'GGB' country code to 'GB'
        users_df_processed['country_code'] = users_df_processed['country_code'].str.replace('GGB', 'GB', regex=False)

        return users_df_processed
    
    def clean_card_data(self, card_df : pd.DataFrame):
        """
        @desc: pre-process the card table
        """
        #   -1) drop columns that are filled with missing and/or incorrect information
        card_processed_df = card_df.drop(columns=['Unnamed: 0'])
        #  -2) always begin with dropping duplicates 
        card_processed_df = card_processed_df.drop_duplicates()
        #   -3) remove columns with "NULL"
        card_processed_df = card_processed_df[~card_processed_df.apply(lambda row: row.astype(str).str.contains('NULL').any(), axis=1)]
        #   -4) remove all entries that are pure alphanumeric
        card_processed_df = card_processed_df[~card_processed_df['card_provider'].apply(is_alphanumeric)]
        #   -5) those rows that have NaN in card_number expiry_date, fill those appropriately
        nan_card_num_expiry_date_df = card_processed_df[card_processed_df['card_number expiry_date'].isna()]
        nan_card_num_expiry_date_df['card_number expiry_date'] = nan_card_num_expiry_date_df['card_number'].astype(str) + ' ' + nan_card_num_expiry_date_df['expiry_date'].astype(str)
        #   -6) those rows that DONT have NaN in card_number expiry_date column, strip those isolated and replace their equivalent NaN values in the card_number and the expiry_date columns appropriately
        not_nan_card_num_expiry_date_df = card_processed_df[~card_processed_df['card_number expiry_date'].isna()]
        splitted_cardnumexpdate_df = not_nan_card_num_expiry_date_df['card_number expiry_date'].str.split(n=1, expand=True)
        not_nan_card_num_expiry_date_df['card_number'], not_nan_card_num_expiry_date_df['expiry_date'] = splitted_cardnumexpdate_df[0], splitted_cardnumexpdate_df[1]
        #   -7) combine the two to store the seperate data with no NaNs . . . (hopefully :P)
        card_processed_df = pd.concat([nan_card_num_expiry_date_df, not_nan_card_num_expiry_date_df], ignore_index=True)
        del nan_card_num_expiry_date_df, not_nan_card_num_expiry_date_df
        #   -8) change all objects to string
        card_processed_df = card_processed_df.astype('string')
        #   -9) finally, change all date columns to datetimeformat
        card_processed_df['date_payment_confirmed'] = convert_date_to_yyyy_mm_dd(card_processed_df['date_payment_confirmed'])
        card_processed_df['expiry_date'] = pd.to_datetime(card_processed_df['expiry_date'], format='%m/%y') + pd.offsets.MonthEnd(0)

        return card_processed_df
    
    def called_clean_store_data(self, store_detail_processed_df : pd.DataFrame):
        #   -1) remove purely nan or none columns (e.g. lat)
        store_detail_processed_df = store_detail_processed_df.drop(columns="lat")
        store_detail_processed_df = store_detail_processed_df.drop(columns="address")
        #   -2) remove all pure alphanmueric rows
        store_detail_processed_df = store_detail_processed_df[~store_detail_processed_df['opening_date'].apply(is_alphanumeric)]
        #   -3) account for missing addresses, longitude and latitude values
        # --> ANS) No need to change, it is a portal type store, and only one and unique in the table
        #   -4) remove all alphabets in staff_numbers column
        store_detail_processed_df["staff_numbers"] = store_detail_processed_df["staff_numbers"].str.replace(r'[a-zA-Z]', '', regex=True)
        #   -5) fix format of opening_date
        store_detail_processed_df["opening_date"] = convert_date_to_yyyy_mm_dd(store_detail_processed_df["opening_date"])
        #   -6) set eeEurope and eeAmerica to Europe and America in the continent column
        store_detail_processed_df["continent"] = store_detail_processed_df["continent"].str.replace('eeEurope', 'Europe')
        store_detail_processed_df["continent"] = store_detail_processed_df["continent"].str.replace('eeAmerica', 'America')
        #   -7) convert all object to string appropriately and all numbers to int and float appropriately
        store_detail_processed_df = store_detail_processed_df.astype({col: 'string' for col in store_detail_processed_df.columns if col not in ["index", "opening_date", "longitude", "staff_numbers", "latitude"]})
        # store_detail_processed_df = store_detail_processed_df.astype({col: 'float64' for col in store_detail_processed_df.columns if col in ["longitude", "latitude"]})
        store_detail_processed_df["longitude"] = pd.to_numeric(store_detail_processed_df["longitude"], errors='coerce')
        store_detail_processed_df["latitude"] = pd.to_numeric(store_detail_processed_df["latitude"], errors='coerce')
        store_detail_processed_df["staff_numbers"] = pd.to_numeric(store_detail_processed_df["staff_numbers"], errors='coerce')

        return store_detail_processed_df