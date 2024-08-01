import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose
from scipy.stats import wilcoxon
from typing import Dict, Any

def analyze_trend_changes(
    original_df: pd.DataFrame,
    new_df: pd.DataFrame,
    column_name: str,
    time_column: str,
    period: int
) -> Dict[str, Any]:
    """
    Analyzes changes in trends for the given time series column after new data insertion.
    
    Parameters
    ----------
    original_df : pd.DataFrame
        The original dataset before new data is inserted.
    new_df : pd.DataFrame
        The new data to be analyzed.
    column_name : str
        The name of the time series column to be analyzed.
    time_column : str
        The column name representing time (e.g., a date or timestamp).
    period : int
        The frequency of the time series for seasonal decomposition.
    
    Returns
    -------
    Dict[str, Any]
        A dictionary containing the trend significance result for the time series column.
    """
    original_series = original_df.set_index(time_column)[column_name]
    new_series = new_df.set_index(time_column)[column_name]
    combined_series = pd.concat([original_series, new_series]).reset_index(drop=True)
    
    decomposition = seasonal_decompose(combined_series, period=period)
    trend = decomposition.trend.dropna()
    
    if len(trend) > 1:
        trend_change_pvalue = wilcoxon(trend[:-1], trend[1:]).pvalue
        trend_significant_change = trend_change_pvalue < 0.05
    else:
        trend_significant_change = False
    
    return {'trend_significant_change': trend_significant_change}

def detect_new_categorical_values(
    original_df: pd.DataFrame,
    new_df: pd.DataFrame,
    column_name: str
) -> Dict[str, Any]:
    """
    Detects new values in a categorical column when new data is inserted into the dataset.
    
    Parameters
    ----------
    original_df : pd.DataFrame
        The DataFrame containing the original dataset.
    new_df : pd.DataFrame
        The DataFrame containing the new data to be added.
    column_name : str
        The name of the categorical column to check for new values.
    
    Returns
    -------
    Dict[str, Any]
        A dictionary indicating if the categorical column has new values and what those values are.
    """
    original_values = set(original_df[column_name].unique())
    new_values = set(new_df[column_name].unique())
    new_entries = new_values - original_values
    
    return {'new_values': list(new_entries)} if new_entries else {}
