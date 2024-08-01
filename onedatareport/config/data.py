
from typing import Optional
from dataclasses import dataclass
from pyspark.sql import SparkSession


@dataclass
class DataConfig:
    """
    Configuration class for specifying data format, type, and source path.

    Attributes
    ----------
    data_format : str
        The format of the data to be read or written (e.g., 'pandas', 'polars', 'pyspark').
    data_type : str
        The type of data file (e.g., 'csv', 'parquet', 'delta').
    path : Optional[str], optional
        The file path or URI from which the data will be read or to which it will be written. 
        This can be a local path, S3 URI, or HTTP URL.
    spark : Optional[SparkSession], optional
        The Spark session to be used if the data format is 'pyspark'.
    """
    data_format: str
    data_type: str
    path: Optional[str] = None
    spark: Optional[SparkSession] = None
