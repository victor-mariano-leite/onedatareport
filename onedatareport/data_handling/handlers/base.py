import pandas as pd
from typing import Union
import polars as pl
from pyspark.sql import DataFrame as PySparkDataFrame
from abc import ABC, abstractmethod

class DataHandler(ABC):
    """
    Abstract base class for handling the reading and writing of data files.

    Methods
    -------
    read(path: str, **kwargs) -> Union[pd.DataFrame, pl.DataFrame, PySparkDataFrame, ColumnarDataFrame]
        Abstract method to read data from the specified path.
    
    write(df: Union[pd.DataFrame, pl.DataFrame, PySparkDataFrame], path: str, **kwargs)
        Abstract method to write data to the specified path.
    """
    
    @abstractmethod
    def read(self, path: str, **kwargs) -> Union[pd.DataFrame, pl.DataFrame, 'PySparkDataFrame', 'ColumnarDataFrame']:
        """
        Read data from the specified path.

        Parameters
        ----------
        path : str
            The file path from which to read the data.
        **kwargs : dict
            Additional keyword arguments to pass to the read function.

        Returns
        -------
        Union[pd.DataFrame, pl.DataFrame, PySparkDataFrame, ColumnarDataFrame]
            The data read from the specified path, returned in the appropriate format.
        """
        pass

    @abstractmethod
    def write(self, df: Union[pd.DataFrame, pl.DataFrame, 'PySparkDataFrame'], path: str, **kwargs):
        """
        Write data to the specified path.

        Parameters
        ----------
        df : Union[pd.DataFrame, pl.DataFrame, PySparkDataFrame]
            The data to be written to the specified path.
        path : str
            The file path to which to write the data.
        **kwargs : dict
            Additional keyword arguments to pass to the write function.
        """
        pass