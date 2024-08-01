
from typing import Dict, Any
from dataclasses import dataclass

@dataclass
class ColumnAnalysisConfig:
    """
    Configuration class for analyzing a single column in the dataset.

    Attributes
    ----------
    column_name : str
        The name of the column to be analyzed.
    time_column : str
        The name of the time column to be used for time series analysis.
    period : int
        The periodicity of the time series data (e.g., 12 for monthly data with yearly seasonality).
    type_schema : Dict[str, Any]
        A dictionary specifying the type of each column in the dataset, where keys are column names 
        and values are types (e.g., 'categorical', 'numeric', 'timeseries').
    """
    column_name: str
    time_column: str
    period: int
    type_schema: Dict[str, Any]


@dataclass
class ColumnsAnalysisConfig:
    """
    Configuration class for analyzing multiple columns in the dataset.

    Attributes
    ----------
    time_column : str
        The name of the time column to be used for time series analysis.
    period : int
        The periodicity of the time series data (e.g., 12 for monthly data with yearly seasonality).
    type_schema : Dict[str, Any]
        A dictionary specifying the type of each column in the dataset, where keys are column names 
        and values are types (e.g., 'categorical', 'numeric', 'timeseries').
    """
    time_column: str
    period: int
    type_schema: Dict[str, Any]