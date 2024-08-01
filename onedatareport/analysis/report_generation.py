import pandas as pd
from typing import Union
from onedatareport.analysis.column import process_columns
from onedatareport.config.analysis import ColumnsAnalysisConfig
from onedatareport.config.data import DataConfig
from onedatareport.data_handling.factory import DataHandlerFactory
import polars as pl
from pyspark.sql import DataFrame as PySparkDataFrame
from typing import Union
import pandas as pd

from utils.data import read_and_prepare_data

def save_final_report(
    final_report_df: Union[pd.DataFrame, pl.DataFrame, PySparkDataFrame],
    config: DataConfig,
    **kwargs
) -> Union[pd.DataFrame, pl.DataFrame, PySparkDataFrame]:
    """
    Save the final report according to the specified format and path.

    Returns the final report DataFrame in the specified format.

    Parameters
    ----------
    final_report_df : Union[pd.DataFrame, pl.DataFrame, PySparkDataFrame]
        The DataFrame containing the final report data.
    config : DataConfig
        The configuration object containing the format, data_type, and path for the final report.

    Returns
    -------
    Union[pd.DataFrame, pl.DataFrame, PySparkDataFrame]
        The final report in the specified format.
    """
    if config.path:
        handler = DataHandlerFactory.get_handler(config)
        handler.write(final_report_df, config.path, **kwargs)
    return final_report_df

    
def generate_report(
    original_config: DataConfig,
    new_config: DataConfig,
    columns_config: ColumnsAnalysisConfig,
    report_config: DataConfig,
    **kwargs
) -> Union[pd.DataFrame, pl.DataFrame, PySparkDataFrame]:
    """
    Generates a final report by processing one column at a time, saving intermediate results, and
    outputting the final report in the specified format.

    Parameters
    ----------
    original_config : DataConfig
        Configuration for the original dataset before new data is inserted.
    new_config : DataConfig
        Configuration for the new data to be analyzed.
    columns_config : ColumnsAnalysisConfig
        Configuration for analyzing multiple columns in the dataset.
    report_config : DataConfig
        Configuration for the final report output (format, data type, path).
    **kwargs :
        Additional keyword arguments to pass to the read functions.

    Returns
    -------
    Union[pd.DataFrame, pl.DataFrame, PySparkDataFrame]
        A DataFrame containing the full report with results for all columns.
    """
    # Step 1: Read and prepare data
    original_df = read_and_prepare_data(original_config, **kwargs)
    new_df = read_and_prepare_data(new_config, **kwargs)

    # Step 2: Process columns and generate report DataFrame
    final_report_df = process_columns(original_df, new_df, columns_config)

    # Step 3: Save and return the final report
    return save_final_report(final_report_df, report_config)
