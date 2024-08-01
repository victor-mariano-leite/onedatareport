import pandas as pd
from typing import Union, Tuple
from onedatareport.data_handling.columnar_dataframe import ColumnarDataFrame
from onedatareport.data_handling.factory import DataHandlerFactory
from onedatareport.config.data import DataConfig
import polars as pl
from pyspark.sql import DataFrame as PySparkDataFrame
from typing import Union
import pandas as pd

def read_data(config: DataConfig, **kwargs) -> Union[pd.DataFrame, pl.DataFrame, PySparkDataFrame, 'ColumnarDataFrame']:
    """
    Reads data based on the DataConfig object and returns it in the specified format.

    Parameters
    ----------
    config : DataConfig
        The DataConfig object containing format, data_type, and path.

    **kwargs :
        Additional keyword arguments to pass to the read functions.

    Returns
    -------
    Union[pd.DataFrame, pl.DataFrame, PySparkDataFrame, 'ColumnarDataFrame']
        The data read from the source in the specified format.
    """
    if config.path and (config.path.startswith('http://') or config.path.startswith('https://')):
        remote_handler = DataHandlerFactory.get_handler(config)
        source_path = remote_handler.download(config.path)
    else:
        source_path = config.path

    handler = DataHandlerFactory.get_handler(config)
    return handler.read(source_path, **kwargs)

def read_and_prepare_data(
    original_config: DataConfig,
    new_config: DataConfig,
    **kwargs
) -> Tuple[Union[pd.DataFrame, ColumnarDataFrame], Union[pd.DataFrame, ColumnarDataFrame]]:
    """
    Read and prepare original and new data according to the specified formats.

    Returns the prepared dataframes for further processing.
    """
    original_df = read_data(original_config, **kwargs)
    new_df = read_data(new_config, **kwargs)
    return original_df, new_df
