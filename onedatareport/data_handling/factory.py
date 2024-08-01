
from onedatareport.data_handling.handlers.pandas import PandasCSVHandler, PandasParquetHandler
from onedatareport.data_handling.handlers.polars import PolarsCSVHandler, PolarsParquetHandler
from onedatareport.data_handling.handlers.pyspark import PySparkCSVHandler, PySparkDeltaHandler, PySparkParquetHandler
from onedatareport.data_handling.handlers.remote import HTTPDataHandler
from onedatareport.data_handling.handlers.base import DataHandler
from onedatareport.config.data import DataConfig


class DataHandlerFactory:
    """
    Factory class to create appropriate DataHandler instances based on the DataConfig.

    Methods
    -------
    get_handler(config: DataConfig) -> DataHandler
        Static method to return a DataHandler instance based on the configuration provided.
    """
    
    @staticmethod
    def get_handler(config: DataConfig) -> DataHandler:
        """
        Return a DataHandler instance based on the configuration provided.

        Parameters
        ----------
        config : DataConfig
            The DataConfig object containing the format, type, and path of the data.

        Returns
        -------
        DataHandler
            An instance of a subclass of DataHandler that matches the data format and type specified 
            in the configuration.

        Raises
        ------
        ValueError
            If no suitable handler is found for the given data format and type.
        """
        if config.path and (config.path.startswith('http://') or config.path.startswith('https://')):
            return HTTPDataHandler()
        if config.data_format == 'pandas':
            if config.data_type == 'csv':
                return PandasCSVHandler()
            elif config.data_type == 'parquet':
                return PandasParquetHandler()
        elif config.data_format == 'polars':
            if config.data_type == 'csv':
                return PolarsCSVHandler()
            elif config.data_type == 'parquet':
                return PolarsParquetHandler()
        elif config.data_format == 'pyspark':
            if config.data_type == 'csv':
                return PySparkCSVHandler(config.spark)
            elif config.data_type == 'parquet':
                return PySparkParquetHandler(config.spark)
            elif config.data_type == 'delta':
                return PySparkDeltaHandler(config.spark)
        raise ValueError(f"No handler found for format: {config.data_format}, type: {config.data_type}")