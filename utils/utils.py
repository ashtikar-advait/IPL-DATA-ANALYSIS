"""
Reusable PySpark utility functions for the IPL Data Engineering project.

These helpers use the SparkSession already provided by Databricks.
This module does not create a SparkSession.
"""

from pyspark.sql import functions as F

DEFAULT_NULL_PLACEHOLDERS = ["NULL", "NA", "N/A", "UNKNOWN", " ", "", "-", "--"]


def getRowsCols(dataframe):
    """Print the total number of rows and columns in a DataFrame."""
    print(f"Total Rows: {dataframe.count():,}")
    print(f"Total Columns: {len(dataframe.columns)}")


def getNullReport(dataframe, null_placeholders=None, columns=None):
    """
    Return a per-column report of actual nulls and placeholder nulls.
    Placeholder checks are applied only to string columns.
    """
    if null_placeholders is None:
        null_placeholders = DEFAULT_NULL_PLACEHOLDERS
    if columns is None:
        columns = dataframe.columns

    total_rows = dataframe.count()
    string_columns = set(getStringTypeCols(dataframe))
    placeholders = [str(value).strip().lower() for value in null_placeholders]
    report = []

    for column_name in columns:
        actual_nulls = dataframe.filter(F.col(column_name).isNull()).count()

        if column_name in string_columns:
            condition = (
                F.col(column_name).isNotNull()
                & F.lower(F.trim(F.col(column_name))).isin(placeholders)
            )
            placeholder_nulls = dataframe.filter(condition).count()
        else:
            placeholder_nulls = 0

        total_bad = actual_nulls + placeholder_nulls
        report.append((
            column_name,
            actual_nulls,
            placeholder_nulls,
            total_bad,
            total_rows - total_bad,
        ))

    schema = """
        column_name STRING,
        actual_nulls LONG,
        placeholder_nulls LONG,
        total_bad_records LONG,
        total_valid_records LONG
    """
    return dataframe.sparkSession.createDataFrame(report, schema=schema)


def getProfile(dataframe, columns=None):
    """Show the 50 most frequent values for each selected column."""
    if columns is None:
        columns = dataframe.columns

    for column_name in columns:
        print("=" * 10)
        print(f"Column: {column_name}")
        print("=" * 10)
        (
            dataframe.groupBy(column_name)
            .count()
            .orderBy(F.desc("count"))
            .show(50, truncate=False)
        )


def stdColName(dataframe):
    """Convert all DataFrame column names to lowercase."""
    return dataframe.toDF(*[name.lower() for name in dataframe.columns])


def getStringTypeCols(dataframe):
    """Return the names of all string-type columns."""
    return [
        name for name, data_type in dataframe.dtypes
        if data_type == "string"
    ]


def stdTrimCols(dataframe, string_type_columns):
    """Trim leading and trailing whitespace from the specified columns."""
    for column_name in string_type_columns:
        dataframe = dataframe.withColumn(
            column_name, F.trim(F.col(column_name))
        )
    return dataframe


def stdCatCols(dataframe, columns=None):
    """Trim and convert selected string columns to title case."""
    if columns is None:
        columns = getStringTypeCols(dataframe)

    for column_name in columns:
        dataframe = dataframe.withColumn(
            column_name, F.initcap(F.trim(F.col(column_name)))
        )
    return dataframe


def handleGarbage(dataframe, columns=None):
    """Remove non-printable characters from selected string columns."""
    if columns is None:
        columns = getStringTypeCols(dataframe)

    for column_name in columns:
        dataframe = dataframe.withColumn(
            column_name,
            F.regexp_replace(F.trim(F.col(column_name)), r"[^\x20-\x7E]", ""),
        )
    return dataframe


def handleNull(dataframe, columns=None, null_placeholders=None):
    """
    Replace actual nulls and configured placeholder values with null.

    String columns use case-insensitive, whitespace-trimmed matching.
    Non-string columns can also be checked against typed placeholders
    (for example, -1 in an integer surrogate-key column).
    """
    if null_placeholders is None:
        null_placeholders = DEFAULT_NULL_PLACEHOLDERS
    if columns is None:
        columns = dataframe.columns

    string_placeholders = [
        str(value).strip().lower()
        for value in null_placeholders
        if isinstance(value, str)
    ]
    non_string_placeholders = [
        value for value in null_placeholders
        if not isinstance(value, str)
    ]
    string_columns = set(getStringTypeCols(dataframe))

    for column_name in columns:
        column = F.col(column_name)
        bad_condition = column.isNull()

        if column_name in string_columns:
            if string_placeholders:
                bad_condition = (
                    bad_condition
                    | F.lower(F.trim(column)).isin(string_placeholders)
                )
        elif non_string_placeholders:
            bad_condition = bad_condition | column.isin(non_string_placeholders)

        dataframe = dataframe.withColumn(
            column_name,
            F.when(bad_condition, F.lit(None)).otherwise(column),
        )

    return dataframe
