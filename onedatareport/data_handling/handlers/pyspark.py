
from pyspark.sql import DataFrame as PySparkDataFrame

from onedatareport.data_handling.handlers.base import DataHandler


class PySparkCSVHandler(DataHandler):
    """
    A handler for reading and writing CSV files using PySpark.

    Methods
    -------
    read(path: str, **kwargs) -> PySparkDataFrame
        Reads a CSV file into a PySpark DataFrame.
    
    write(df: PySparkDataFrame, path: str, **kwargs)
        Writes a PySpark DataFrame to a CSV file.
    """
    
    def __init__(self, spark):
        """
        Initialize the handler with a Spark session.

        Parameters
        ----------
        spark : SparkSession
            The active Spark session used to read and write data.
        """
        self.spark = spark

    def read(self, path: str, **kwargs) -> 'PySparkDataFrame':
        """
        Reads a CSV file into a PySpark DataFrame.

        Parameters
        ----------
        path : str
            The file path of the CSV to be read.
        **kwargs : dict
            Additional keyword arguments to pass to `spark.read.csv`.

        Returns
        -------
        PySparkDataFrame
            The data read from the CSV file.
        """
        return self.spark.read.csv(path, header=True, inferSchema=True, **kwargs)

    def write(self, df: 'PySparkDataFrame', path: str, **kwargs):
        """
        Writes a PySpark DataFrame to a CSV file.

        Parameters
        ----------
        df : PySparkDataFrame
            The PySpark DataFrame to be written to a CSV file.
        path : str
            The file path where the CSV will be saved.
        **kwargs : dict
            Additional keyword arguments to pass to `df.write.csv`.
        """
        df.write.csv(path, header=True, **kwargs)


class PySparkParquetHandler(DataHandler):
    """
    A handler for reading and writing Parquet files using PySpark.

    Methods
    -------
    read(path: str, **kwargs) -> PySparkDataFrame
        Reads a Parquet file into a PySpark DataFrame.
    
    write(df: PySparkDataFrame, path: str, **kwargs)
        Writes a PySpark DataFrame to a Parquet file.
    """
    
    def __init__(self, spark):
        """
        Initialize the handler with a Spark session.

        Parameters
        ----------
        spark : SparkSession
            The active Spark session used to read and write data.
        """
        self.spark = spark

    def read(self, path: str, **kwargs) -> 'PySparkDataFrame':
        """
        Reads a Parquet file into a PySpark DataFrame.

        Parameters
        ----------
        path : str
            The file path of the Parquet file to be read.
        **kwargs : dict
            Additional keyword arguments to pass to `spark.read.parquet`.

        Returns
        -------
        PySparkDataFrame
            The data read from the Parquet file.
        """
        return self.spark.read.parquet(path, **kwargs)

    def write(self, df: 'PySparkDataFrame', path: str, **kwargs):
        """
        Writes a PySpark DataFrame to a Parquet file.

        Parameters
        ----------
        df : PySparkDataFrame
            The PySpark DataFrame to be written to a Parquet file.
        path : str
            The file path where the Parquet file will be saved.
        **kwargs : dict
            Additional keyword arguments to pass to `df.write.parquet`.
        """
        df.write.parquet(path, **kwargs)


class PySparkDeltaHandler(DataHandler):
    """
    A handler for reading and writing Delta Lake files using PySpark.

    Methods
    -------
    read(path: str, **kwargs) -> PySparkDataFrame
        Reads a Delta Lake table into a PySpark DataFrame.
    
    write(df: PySparkDataFrame, path: str, **kwargs)
        Writes a PySpark DataFrame to a Delta Lake table.
    """
    
    def __init__(self, spark):
        """
        Initialize the handler with a Spark session.

        Parameters
        ----------
        spark : SparkSession
            The active Spark session used to read and write data.
        """
        self.spark = spark

    def read(self, path: str, **kwargs) -> 'PySparkDataFrame':
        """
        Reads a Delta Lake table into a PySpark DataFrame.

        Parameters
        ----------
        path : str
            The file path of the Delta Lake table to be read.
        **kwargs : dict
            Additional keyword arguments to pass to `spark.read.format("delta").load`.

        Returns
        -------
        PySparkDataFrame
            The data read from the Delta Lake table.
        """
        return self.spark.read.format("delta").load(path, **kwargs)

    def write(self, df: 'PySparkDataFrame', path: str, **kwargs):
        """
        Writes a PySpark DataFrame to a Delta Lake table.

        Parameters
        ----------
        df : PySparkDataFrame
            The PySpark DataFrame to be written to a Delta Lake table.
        path : str
            The file path where the Delta Lake table will be saved.
        **kwargs : dict
            Additional keyword arguments to pass to `df.write.format("delta").save`.
        """
        df.write.format("delta").save(path, **kwargs)

