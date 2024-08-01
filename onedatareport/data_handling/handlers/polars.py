from onedatareport.data_handling.handlers.base import DataHandler
import polars as pl

class PolarsCSVHandler(DataHandler):
    """
    A handler for reading and writing CSV files using Polars.

    Methods
    -------
    read(path: str, **kwargs) -> pl.DataFrame
        Reads a CSV file into a Polars DataFrame.
    
    write(df: pl.DataFrame, path: str, **kwargs)
        Writes a Polars DataFrame to a CSV file.
    """
    
    def read(self, path: str, **kwargs) -> pl.DataFrame:
        """
        Reads a CSV file into a Polars DataFrame.

        Parameters
        ----------
        path : str
            The file path of the CSV to be read.
        **kwargs : dict
            Additional keyword arguments to pass to `polars.read_csv`.

        Returns
        -------
        pl.DataFrame
            The data read from the CSV file.
        """
        return pl.read_csv(path, **kwargs)

    def write(self, df: pl.DataFrame, path: str, **kwargs):
        """
        Writes a Polars DataFrame to a CSV file.

        Parameters
        ----------
        df : pl.DataFrame
            The Polars DataFrame to be written to a CSV file.
        path : str
            The file path where the CSV will be saved.
        **kwargs : dict
            Additional keyword arguments to pass to `polars.DataFrame.write_csv`.
        """
        df.write_csv(path, **kwargs)


class PolarsParquetHandler(DataHandler):
    """
    A handler for reading and writing Parquet files using Polars.

    Methods
    -------
    read(path: str, **kwargs) -> pl.DataFrame
        Reads a Parquet file into a Polars DataFrame.
    
    write(df: pl.DataFrame, path: str, **kwargs)
        Writes a Polars DataFrame to a Parquet file.
    """
    
    def read(self, path: str, **kwargs) -> pl.DataFrame:
        """
        Reads a Parquet file into a Polars DataFrame.

        Parameters
        ----------
        path : str
            The file path of the Parquet file to be read.
        **kwargs : dict
            Additional keyword arguments to pass to `polars.read_parquet`.

        Returns
        -------
        pl.DataFrame
            The data read from the Parquet file.
        """
        return pl.read_parquet(path, **kwargs)

    def write(self, df: pl.DataFrame, path: str, **kwargs):
        """
        Writes a Polars DataFrame to a Parquet file.

        Parameters
        ----------
        df : pl.DataFrame
            The Polars DataFrame to be written to a Parquet file.
        path : str
            The file path where the Parquet file will be saved.
        **kwargs : dict
            Additional keyword arguments to pass to `polars.DataFrame.write_parquet`.
        """
        df.write_parquet(path, **kwargs)

