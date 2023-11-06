from data_extraction import DataExtractor
from dateutil.parser import parse
import pandas as pd
import re
class DataCleaning(DataExtractor):
    """
    @desc : methods to clean data from each of the data sources
    """
    def __init__(self):
        super().__init__()
    # ================================================================== #
    #                   UTILITY FUNCTIONS
    # ================================================================== #
    @staticmethod
    def convert_weights(weight):
        """
        @desc: check if the input matches the numeric and alphabet matching expression
        """
        match = re.match(r"([\d.]+)([a-zA-Z]+)", weight)
        # if there is match then set the first output as value (numeric) and second as unit (g, kg, ml or l)
        if match:
            value, unit = match.groups()
            # conver the numeric value to floating pt
            value = float(value)
            # check for the cases of 'g', 'ml' and 'l'
            if unit == 'g':
                value /= 1000
                unit = 'kg'
            elif unit == 'ml':
                value /= 1000
                unit = 'kg'
            elif unit == 'l':
                unit = 'kg'
            elif unit == 'oz':
                value *= 0.0283495
                unit = 'kg'
            # force the output to be 3 d.p
            return f'{value:.3f}{unit}'
        else:
            return weight
    
    @staticmethod
    def mullexp_to_netresult(in_exp):
        if 'x' in in_exp:
            match = re.match(r'(\d+)\s*x\s*(\d+)([a-zA-Z]+)', in_exp)
            if match:
                multiplier = int(match.group(1))
                value = int(match.group(2))
                unit = match.group(3)
                # Perform the multiplication
                result = multiplier * value
                # Append the result with the unit
                return str(result) + unit
        else:
            return in_exp
        

    @staticmethod
    def is_alpha(in_str):
        """
        @desc: function to check if the column has ONLY alphabets entries
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
    def clean_orders_data(self):
        """
        @desc: pre-process the orders table
        """
        order_processed_df = super().read_rds_table('orders_table').copy().drop_duplicates()
        order_processed_df = order_processed_df.drop(columns={'first_name', 'last_name', '1'})
        
        return order_processed_df

    def clean_user_data(self, table_name : str = 'legacy_users'):
        """
        @desc: pre-process the legacy users table
        """
        users_df = super().read_rds_table('legacy_users')
        if users_df.isnull().sum().sum() and users_df.isna().sum().sum():
            raise f"The database : {table_name}, has total {users_df.isnull().sum().sum()} NULL values and {users_df.isna().sum().sum()} NaN values"
        else:
            print(f"[usrmsg] No NULLs or NaNs found in {table_name}")
        users_df_processed = users_df.copy()
        users_df_processed = users_df[~users_df.apply(lambda row: row.astype(str).str.contains('NULL').any(), axis=1)]
        # check for data types 
        #   -1) always begin with dropping duplicates and storing as a seperate file
        users_df_processed = users_df_processed.drop_duplicates()
        #   -2) set all columns except index to be of string format
        str_convert_dict = {col: 'string' for col in users_df_processed.columns if col not in ['index']}
        users_df_processed = users_df_processed.astype(str_convert_dict)
        #   -3) remove all entries that are pure alphanumeric
        users_df_processed = users_df_processed[~users_df_processed['email_address'].apply(self.is_alphanumeric)]
        #   -4) DoB and join_date should be datetime format and of type yyyy-mm-dd
        users_df_processed['date_of_birth'] = self.convert_date_to_yyyy_mm_dd(users_df_processed['date_of_birth'])
        users_df_processed['join_date'] = self.convert_date_to_yyyy_mm_dd(users_df_processed['join_date'])
        #   -5) convert all 'GGB' country code to 'GB'
        users_df_processed['country_code'] = users_df_processed['country_code'].str.replace('GGB', 'GB', regex=False)

        return users_df_processed
    
    def clean_card_data(self):
        """
        @desc: pre-process the card table
        """
        #   -1) drop columns that are filled with missing and/or incorrect information
        card_processed_df = super().retrieve_pdf_data('../card_details.pdf').copy().drop(columns=['Unnamed: 0'])
        #  -2) always begin with dropping duplicates 
        card_processed_df = card_processed_df.drop_duplicates()
        #   -3) remove columns with "NULL"
        card_processed_df = card_processed_df[~card_processed_df.apply(lambda row: row.astype(str).str.contains('NULL').any(), axis=1)]
        #   -4) remove all entries that are pure alphanumeric
        card_processed_df = card_processed_df[~card_processed_df['card_provider'].apply(self.is_alphanumeric)]
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
        card_processed_df['date_payment_confirmed'] = self.convert_date_to_yyyy_mm_dd(card_processed_df['date_payment_confirmed'])
        card_processed_df['expiry_date'] = pd.to_datetime(card_processed_df['expiry_date'], format='%m/%y') + pd.offsets.MonthEnd(0)

        return card_processed_df
    
    def called_clean_store_data(self):
        """
        @desc: pre-process the store table
        """
        store_detail_processed_df = super().retrieve_stores_data().copy()
        #   -1) always begin with removing duplicates
        store_detail_processed_df = store_detail_processed_df.drop_duplicates()
        #   -2) remove purely nan or none columns (e.g. lat)
        store_detail_processed_df = store_detail_processed_df.drop(columns="lat")
        store_detail_processed_df = store_detail_processed_df.drop(columns="address")
        #   -3) remove all pure alphanmueric rows
        store_detail_processed_df = store_detail_processed_df[~store_detail_processed_df['opening_date'].apply(self.is_alphanumeric)]
        #   -4) account for missing addresses, longitude and latitude values
        # --> ANS) No need to change, it is a portal type store, and only one and unique in the table
        #   -5) remove all alphabets in staff_numbers column
        store_detail_processed_df["staff_numbers"] = store_detail_processed_df["staff_numbers"].str.replace(r'[a-zA-Z]', '', regex=True)
        #   -6) fix format of opening_date
        store_detail_processed_df["opening_date"] = self.convert_date_to_yyyy_mm_dd(store_detail_processed_df["opening_date"])
        #   -7) set eeEurope and eeAmerica to Europe and America in the continent column
        store_detail_processed_df["continent"] = store_detail_processed_df["continent"].str.replace('eeEurope', 'Europe')
        store_detail_processed_df["continent"] = store_detail_processed_df["continent"].str.replace('eeAmerica', 'America')
        #   -8) convert all object to string appropriately and all numbers to int and float appropriately
        store_detail_processed_df = store_detail_processed_df.astype({col: 'string' for col in store_detail_processed_df.columns if col not in ["index", "opening_date", "longitude", "staff_numbers", "latitude"]})
        # store_detail_processed_df = store_detail_processed_df.astype({col: 'float64' for col in store_detail_processed_df.columns if col in ["longitude", "latitude"]})
        store_detail_processed_df["longitude"] = pd.to_numeric(store_detail_processed_df["longitude"], errors='coerce')
        store_detail_processed_df["latitude"] = pd.to_numeric(store_detail_processed_df["latitude"], errors='coerce')
        store_detail_processed_df["staff_numbers"] = pd.to_numeric(store_detail_processed_df["staff_numbers"], errors='coerce')

        return store_detail_processed_df


    def clean_products_data(self):
        """
        @desc: pre-process the store table
        """
        products_processed_df = super().extract_from_s3().copy()
        #   -1) always begin by dropping duplicates
        products_processed_df = products_processed_df.drop_duplicates()
        #   -2) rename the Unamed column to index
        products_processed_df.rename(columns={'Unnamed: 0': 'index'}, inplace=True)
        #   -3) fill nans with empty strings to allow easier processing for the rest of the cleaning
        products_processed_df = products_processed_df.fillna('')
        #   -4) remove all the pure alphanumeric entries
        products_processed_df = products_processed_df[~products_processed_df['date_added'].apply(self.is_alphanumeric)]
        #   -5) for weights : a) compute all multiplication expressions and replace with resultant value
        products_processed_df["weight"] = products_processed_df["weight"].apply(self.mullexp_to_netresult)
        #   -6) for weights : b) standardise them to 'kg'
        products_processed_df["weight"] = products_processed_df["weight"].apply(self.convert_weights)
        #   -7) drop £ and kg to enable weight and product price columns to be numeric
        products_processed_df["product_price"] = products_processed_df["product_price"].str.replace('£', '')
        products_processed_df.rename(columns={'product_price': 'product_price (£)'}, inplace=True)
        products_processed_df["weight"] = products_processed_df["weight"].str.replace('kg', '')
        products_processed_df.rename(columns={'weight': 'weight (kg)'}, inplace=True)
        #   -8) set product_price, weight, EAN to be numeric
        products_processed_df["product_price (£)"] = pd.to_numeric(products_processed_df["product_price (£)"], errors='coerce')
        products_processed_df["weight (kg)"] = pd.to_numeric(products_processed_df["weight (kg)"], errors='coerce')
        products_processed_df["EAN"] = pd.to_numeric(products_processed_df["EAN"], errors='coerce')
        #   -9) set product_name, cateogry, uuid, removed and product_code to string  AND  date_added to datetime
        products_processed_df = products_processed_df.astype({"product_name" : "string", "category" : "string", "uuid" : "string", "removed" : "string", "product_code" : "string"})
        products_processed_df['date_added'] = self.convert_date_to_yyyy_mm_dd(products_processed_df['date_added'])

        return products_processed_df
    

    def clean_event_date_data(self):
        """
        @desc: pre-process the date details table
        """
        #   -1) drop duplicates and store a copy of the original
        events_df_processed = super().extract_eventstable_from_s3().copy().drop_duplicates()
        #   -2) remove all entries that are purely alphanumeric in nature
        events_df_processed = events_df_processed[~events_df_processed["date_uuid"].apply(self.is_alphanumeric)]
        #   -3) use info from year, month, day and timestamp to set a seperate datetime column
        events_df_processed['datetime'] = pd.to_datetime(events_df_processed[['year', 'month', 'day', 'timestamp']].astype(str).agg(' '.join, axis=1), format='%Y %m %d %H:%M:%S')
        #   -4) set timestamp, timeperiod and date_uuid as string
        events_df_processed = events_df_processed.astype({"timestamp" : "string", "time_period" : "string", "date_uuid" : "string"})
        #   -5) set month, year and day as int32
        events_df_processed["month"] = pd.to_numeric(events_df_processed["month"], errors='coerce')
        events_df_processed["year"] = pd.to_numeric(events_df_processed["year"], errors='coerce')
        events_df_processed["day"] = pd.to_numeric(events_df_processed["day"], errors='coerce')

        return events_df_processed