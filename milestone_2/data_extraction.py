from database_utils import DatabaseConnector

class DataExtractor(DatabaseConnector):
    """
    @desc : This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
    
    The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.
    """
    def __init__(self):
        # instantiate the Parent class DatabaseConnector
        super.__init__(self)
    
    def read_rds_table(self):
        return super.init_db_engine()