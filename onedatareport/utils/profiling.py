import pandas as pd
from ydata_profiling import ProfileReport
from typing import Dict, Any, Optional
import json
import pandas as pd

def filter_nested_fields(details: Dict[str, Any], fields_to_keep: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively filters the fields in a nested dictionary based on a list of fields to keep.
    
    Parameters
    ----------
    details : Dict[str, Any]
        The dictionary containing the details of a variable from the profiling data.
    fields_to_keep : Dict[str, Any]
        A dictionary where keys are the fields to keep and values can be nested dictionaries 
        for further filtering or True for terminal fields.
    
    Returns
    -------
    Dict[str, Any]
        A filtered dictionary containing only the fields specified in fields_to_keep.
    """
    filtered = {}
    for key, value in details.items():
        if key in fields_to_keep:
            if isinstance(value, dict) and isinstance(fields_to_keep[key], dict):
                filtered[key] = filter_nested_fields(value, fields_to_keep[key])
            else:
                filtered[key] = value
    return filtered

def flatten_dict(data: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """
    Recursively flattens a nested dictionary.
    
    Parameters
    ----------
    data : Dict[str, Any]
        The nested dictionary to be flattened.
    parent_key : str, optional
        The base key to use for flattening, by default ''.
    sep : str, optional
        The separator between keys when flattening, by default '_'.
    
    Returns
    -------
    Dict[str, Any]
        The flattened dictionary with concatenated keys.
    """
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

# 2. Profiling and Metrics Calculation
def filter_profile_data(profile_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Filters the profiling data to retain only specific fields based on the data type of each variable.
    
    Parameters
    ----------
    profile_data : Dict[str, Any]
        The profiling data extracted from ydata-profiling.
    
    Returns
    -------
    Dict[str, Dict[str, Any]]
        A dictionary containing filtered profiling data for each variable.
    """
    fields_to_keep = {
        "Categorical": {
            "n": True, "n_distinct": True, "p_distinct": True, "is_unique": True, "n_unique": True, "p_unique": True, 
            "ordering": True, "n_missing": True, "p_missing": True, "memory_size": True, "imbalance": True, 
            "max_length": True, "mean_length": True, "median_length": True, "min_length": True,
            "chi_squared": {"statistic": True, "pvalue": True}
        },
        "TimeSeries": {
            "n": True, "n_distinct": True, "p_distinct": True, "is_unique": True, "n_unique": True, "p_unique": True, 
            "ordering": True, "n_missing": True, "p_missing": True, "memory_size": True, "mean": True, 
            "std": True, "variance": True, "min": True, "max": True, "kurtosis": True, "skewness": True, 
            "sum": True, "mad": True, "range": True, "seasonal": True, "stationary": True,
            "chi_squared": {"statistic": True, "pvalue": True},
            "gap_stats": {"min": True, "max": True, "mean": True, "std": True, "n_gaps": True}
        },
        "Numeric": {
            "n": True, "n_distinct": True, "p_distinct": True, "is_unique": True, "n_unique": True, "p_unique": True, 
            "ordering": True, "n_missing": True, "p_missing": True, "memory_size": True, "mean": True, 
            "std": True, "variance": True, "min": True, "max": True, "kurtosis": True, "skewness": True, 
            "sum": True, "mad": True, "range": True, "iqr": True, "cv": True, "p_zeros": True,
            "chi_squared": {"statistic": True, "pvalue": True}
        }
    }
    
    filtered_data = {}
    
    for variable, details in profile_data.get("variables", {}).items():
        data_type = details.get("type")
        if data_type in fields_to_keep:
            filtered_data[variable] = filter_nested_fields(details, fields_to_keep[data_type])
    
    return filtered_data

def json_to_flat_dataframe(json_data: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    """
    Converts a JSON-like structure to a flattened DataFrame where each row corresponds to a variable
    and each column represents a field from the analysis.
    
    Parameters
    ----------
    json_data : Dict[str, Dict[str, Any]]
        The JSON-like structure containing the profiling data and analysis results.
    
    Returns
    -------
    pd.DataFrame
        The flattened DataFrame.
    """
    flattened_data = {}
    
    for variable, details in json_data.items():
        flattened_data[variable] = flatten_dict(details)
    
    df = pd.DataFrame.from_dict(flattened_data, orient='index')
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'column_name'}, inplace=True)
    
    return df

def calculate_observability_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates observability metrics for each column in the flattened DataFrame generated from profiling data.
    
    Parameters
    ----------
    df : pd.DataFrame
        The flattened DataFrame where each row corresponds to a column analyzed, and each column represents
        a field from the profiling data and analysis.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame with additional columns for observability metrics.
    """
    return df.assign(
        # Categorical Metrics
        categorical_cardinality_ratio=lambda x: x['n_distinct'] / x['n'],
        categorical_missingness_impact=lambda x: x['p_missing'] * x['n_distinct'],
        categorical_chi_squared_alert=lambda x: x['chi_squared_pvalue'] < 0.05,
        
        # TimeSeries Metrics
        timeseries_gap_ratio=lambda x: x['gap_stats_n_gaps'] / x['n'],
        timeseries_volatility_index=lambda x: x['std'] / x['mean'],
        timeseries_trend_consistency=lambda x: x['std'] / x['mean'],
        
        # Numeric Metrics
        numeric_zero_ratio=lambda x: x['p_zeros'],
        numeric_outlier_indicator=lambda x: x['range'] / x['std'],
        numeric_skewness_indicator=lambda x: x['skewness'],
        numeric_cv=lambda x: x['cv'],
        numeric_missing_impact=lambda x: x['p_missing'] * x['mean'],
        
        # General Metrics
        data_completeness=lambda x: 1 - x['p_missing']
    )

def extract_profile_data(data_dict: str) -> pd.DataFrame:
    """
    Extracts profiling data from a JSON string, filters it based on predefined criteria,
    flattens the resulting dictionary into a DataFrame, and calculates observability metrics.
    
    Parameters
    ----------
    data_dict : str
        A JSON string containing profiling data, typically generated by a profiling tool like ydata-profiling.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame where each row corresponds to a column from the original data,
        and each column represents a field from the profiling data or derived observability metrics.
    
    Example
    -------
    >>> data_dict = '{"variables": {...}}'
    >>> result_df = extract_profile_data(data_dict)
    """
    filtered_data: Dict[str, Dict[str, Any]] = filter_profile_data(data_dict)
    flattened_data: pd.DataFrame = json_to_flat_dataframe(filtered_data)
    return calculate_observability_metrics(flattened_data)

def run_ydata_profiling_report(
    df: pd.DataFrame,
    type_schema: Dict[str, Any],
    sortby: Optional[str] = None
) -> pd.DataFrame:
    """
    Runs the ydata-profiling report on the given DataFrame, generates the profiling report in JSON format,
    and then extracts and processes the data to calculate observability metrics.
    
    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame containing the data to be profiled.
    type_schema : Dict[str, Any]
        A dictionary specifying the type schema for each column in the DataFrame.
    sortby : Optional[str], optional
        The column name by which to sort the profiling report, by default None.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the profiling data along with calculated observability metrics.
    
    Example
    -------
    >>> df = pd.DataFrame({...})
    >>> type_schema = {"column1": "categorical", "column2": "numeric"}
    >>> result_df = run_ydata_profiling_report(df, type_schema, sortby="column1")
    """
    profile = ProfileReport(
        df,
        tsmode=True,
        explorative=True,
        type_schema=type_schema,
        sortby=sortby,
    )
    json_data: Dict[str, Any] = json.loads(profile.to_json())
    return extract_profile_data(json_data)
