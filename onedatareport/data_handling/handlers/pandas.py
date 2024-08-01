import pandas as pd

from onedatareport.data_handling.handlers.base import DataHandler

class PandasCSVHandler(DataHandler):
    """
    A handler for reading and writing CSV files using Pandas.

    Methods
    -------
    read(path: str, **kwargs) -> pd.DataFrame
        Reads a CSV file into a Pandas DataFrame.
    
    write(df: pd.DataFrame, path: str, **kwargs)
        Writes a Pandas DataFrame to a CSV file.
    """
    
    def read(self, path: str, **kwargs) -> pd.DataFrame:
        """
        Reads a CSV file into a Pandas DataFrame.

        Parameters
        ----------
        path : str
            The file path of the CSV to be read.
        **kwargs : dict
            Additional keyword arguments to pass to `pandas.read_csv`.

        Returns
        -------
        pd.DataFrame
            The data read from the CSV file.
        """
        return pd.read_csv(path, **kwargs)

    def write(self, df: pd.DataFrame, path: str, **kwargs):
        """
        Writes a Pandas DataFrame to a CSV file.

        Parameters
        ----------
        df : pd.DataFrame
            The Pandas DataFrame to be written to a CSV file.
        path : str
            The file path where the CSV will be saved.
        **kwargs : dict
            Additional keyword arguments to pass to `pandas.DataFrame.to_csv`.
        """
        df.to_csv(path, index=False, **kwargs)


class PandasParquetHandler(DataHandler):
    """
    A handler for reading and writing Parquet files using Pandas.

    Methods
    -------
    read(path: str, **kwargs) -> pd.DataFrame
        Reads a Parquet file into a Pandas DataFrame.
    
    write(df: pd.DataFrame, path: str, **kwargs)
        Writes a Pandas DataFrame to a Parquet file.
    """
    
    def read(self, path: str, **kwargs) -> pd.DataFrame:
        """
        Reads a Parquet file into a Pandas DataFrame.

        Parameters
        ----------
        path : str
            The file path of the Parquet file to be read.
        **kwargs : dict
            Additional keyword arguments to pass to `pandas.read_parquet`.

        Returns
        -------
        pd.DataFrame
            The data read from the Parquet file.
        """
        return pd.read_parquet(path, **kwargs)

    def write(self, df: pd.DataFrame, path: str, **kwargs):
        """
        Writes a Pandas DataFrame to a Parquet file.

        Parameters
        ----------
        df : pd.DataFrame
            The Pandas DataFrame to be written to a Parquet file.
        path : str
            The file path where the Parquet file will be saved.
        **kwargs : dict
            Additional keyword arguments to pass to `pandas.DataFrame.to_parquet`.
        """
        df.to_parquet(path, index=False, **kwargs)
