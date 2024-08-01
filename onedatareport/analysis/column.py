import pandas as pd
from typing import Union

from onedatareport.analysis.trend import analyze_trend_changes, detect_new_categorical_values
from onedatareport.config.analysis import ColumnAnalysisConfig, ColumnsAnalysisConfig
from onedatareport.data_handling.columnar_dataframe import ColumnarDataFrame
from onedatareport.utils.profiling import run_ydata_profiling_report


def process_column(
    original_df: pd.DataFrame,
    new_df: pd.DataFrame,
    config: ColumnAnalysisConfig
) -> pd.DataFrame:
    """
    Process a single column and generate analysis for that column.

    Parameters
    ----------
    original_df : pd.DataFrame
        The original dataset before new data is inserted.
    new_df : pd.DataFrame
        The new data to be analyzed.
    config : ColumnAnalysisConfig
        Configuration for analyzing the column.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the results of the analysis for the column.
    """
    result = {'column_name': config.column_name}
    column_type = config.type_schema[config.column_name]

    if column_type == "timeseries":
        trend_changes = analyze_trend_changes(original_df, new_df, config.column_name, config.time_column, config.period)
        result.update(trend_changes)

    elif column_type == "categorical":
        new_values = detect_new_categorical_values(original_df, new_df, config.column_name)
        result.update(new_values)

    profile_data = run_ydata_profiling_report(new_df[[config.column_name]], {config.column_name: column_type})
    if not profile_data.empty:
        result.update(profile_data.iloc[0].to_dict())

    return pd.DataFrame([result])

def process_columns(
    original_df: Union[pd.DataFrame, ColumnarDataFrame],
    new_df: Union[pd.DataFrame, ColumnarDataFrame],
    columns_config: ColumnsAnalysisConfig
) -> pd.DataFrame:
    """
    Iterate over columns and process each, accumulating results in a DataFrame.

    Parameters
    ----------
    original_df : Union[pd.DataFrame, 'ColumnarDataFrame']
        The original dataset before new data is inserted.
    new_df : Union[pd.DataFrame, 'ColumnarDataFrame']
        The new data to be analyzed.
    columns_config : ColumnsAnalysisConfig
        Configuration object for analyzing multiple columns.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the accumulated results for all columns.
    """
    final_report_df = pd.DataFrame()

    for column_name in original_df.columns:
        if column_name == columns_config.time_column:
            continue
        column_config = ColumnAnalysisConfig(
            column_name=column_name,
            time_column=columns_config.time_column,
            period=columns_config.period,
            type_schema=columns_config.type_schema
        )
        if isinstance(original_df, ColumnarDataFrame):
            original_column_df = original_df.load_column(column_name)
            new_column_df = new_df.load_column(column_name)
            column_report_df = process_column(original_column_df, new_column_df, column_config)
        else:
            column_report_df = process_column(original_df[[columns_config.time_column, column_name]], new_df[[columns_config.time_column, column_name]], column_config)
        final_report_df = pd.concat([final_report_df, column_report_df], ignore_index=True)
    return final_report_df

