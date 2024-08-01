import pandas as pd
import os
import pandas as pd
import tempfile
import shutil
from typing import List

class ColumnarDataFrame:
    def __init__(self, df: pd.DataFrame, data_type: str):
        """
        Initialize the ColumnarDataFrame with the original data.

        Parameters
        ----------
        df : pd.DataFrame
            The original DataFrame containing the data.
        data_type : str
            The type of the original data ('pandas', 'polars', 'pyspark').
        """
        self.data_type = data_type
        self.temp_dir = tempfile.mkdtemp()
        self.columns = df.columns if self.data_type == 'pyspark' else df.columns.tolist()
        self.current_column = None
        self.current_column_name = None
        self.store_data(df)

    def store_data(self, df: pd.DataFrame):
        """
        Stores each column of the DataFrame as a separate file on disk.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing the data to store.
        """
        for col in self.columns:
            col_path = os.path.join(self.temp_dir, f"{col}.pkl")
            df[[col]].to_pickle(col_path)

    def load_column(self, column_name: str) -> pd.DataFrame:
        """
        Load a single column into memory as a pandas DataFrame, and store the currently
        loaded column back to disk.

        Parameters
        ----------
        column_name : str
            The name of the column to load.

        Returns
        -------
        pd.DataFrame
            The specified column as a pandas DataFrame.
        """
        if self.current_column_name is not None and self.current_column_name != column_name:
            # Store the currently in-memory column back to disk
            col_path = os.path.join(self.temp_dir, f"{self.current_column_name}.pkl")
            self.current_column.to_pickle(col_path)

        # Load the requested column from disk
        col_path = os.path.join(self.temp_dir, f"{column_name}.pkl")
        self.current_column = pd.read_pickle(col_path)
        self.current_column_name = column_name

        return self.current_column

    def __iter__(self):
        """
        Iterator over the columns of the DataFrame.
        """
        for col in self.columns:
            yield self.load_column(col)

    def get_columns(self) -> List[str]:
        """
        Get the list of columns in the DataFrame.

        Returns
        -------
        List[str]
            The list of columns in the DataFrame.
        """
        return self.columns

    def __del__(self):
        """
        Cleanup the temporary files when the object is deleted.
        """
        shutil.rmtree(self.temp_dir)
