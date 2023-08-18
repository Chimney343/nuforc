def replace_unparsed_with_none(dataframe):
    """
    Function to replace all occurrences of "unparsed" with None in the entire DataFrame.

    Parameters:
        dataframe (pandas DataFrame): The DataFrame to process.

    Returns:
        pandas DataFrame: The DataFrame with "unparsed" replaced by None.
    """
    dataframe = dataframe.replace("unparsed", None)
    return dataframe


def join_columns(city, state, country):
    """
    Function to join 'city', 'state', and 'country' columns and return the result.

    Parameters:
        city_col (pandas Series): The 'city' column from the DataFrame.
        state_col (pandas Series): The 'state' column from the DataFrame.
        country_col (pandas Series): The 'country' column from the DataFrame.

    Returns:
        pandas Series: The joined result of 'city', 'state', and 'country' columns.
    """
    # Check if all input values are not None
    values = [str(val) for val in (city, state, country) if val is not None]

    # Combine the three columns using a comma separator
    return ', '.join(values)