import pandas as pd
from dateutil.parser import parse
class DataCleaning:
    """
    @desc : methods to clean data from each of the data sources
    """
    
    # ================================================================== #
    #                   UTILITY FUNCTIONS
    # ================================================================== #
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



    