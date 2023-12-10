import pandas as pd


def generate_car_matrix(df)->pd.DataFrame:
    """
    Creates a DataFrame  for id combinations.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Matrix generated with 'car' values, 
                          where 'id_1' and 'id_2' are used as indices and columns respectively.
    """
    data = pd.read_csv('dataset-1.csv')
    df= pd.pivot_table(data, values='car', index='id_1', columns='id_2', fill_value=0)
    df.values[[range(len(df))] * 2] = 0

    return df


def get_type_count(df)->dict:
    """
    Categorizes 'car' values into types and returns a dictionary of counts.

    Args:
        df (pandas.DataFrame)

    Returns:
        dict: A dictionary with car types as keys and their counts as values.
    """
    data = pd.read_csv('dataset-1.csv')
    data['car_type'] = pd.cut(data['car'], bins=[-float('inf'), 15, 25, float('inf')],
                              labels=['low', 'medium', 'high'], right=False)

    type_counts = data['car_type'].value_counts().to_dict()

    df = {k: type_counts[k] for k in sorted(type_counts)}

    return df()


def get_bus_indexes(df)->list:

    bus_mean = df['bus'].mean()
    bus_indexes = df[df['bus'] > 2 * bus_mean].index.tolist()

    return bus_indexes


def filter_routes(df)->list:
    """
    Filters and returns routes with average 'truck' values greater than 7.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of route names with average 'truck' values greater than 7.
    """
    df= pd.read_csv('dataset-1.csv')
    route_avg_truck = df.groupby('route')['truck'].mean()
    filtered_routes = route_avg_truck[route_avg_truck > 7].index.tolist()
    filtered_routes.sort()

    return filtered_routes


def multiply_matrix(matrix)->pd.DataFrame:
    """
    Multiplies matrix values with custom conditions.

    Args:
        matrix (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Modified matrix with values multiplied based on custom conditions.
    """

    modified_matrix = matrix.copy()
    modified_matrix[matrix > 20] *= 0.75
    modified_matrix[(matrix <= 20) & (matrix >= 0)] *= 1.25
    modified_matrix = modified_matrix.round(1)

    return modified_matrix


def time_check(df)->pd.Series:
    """
    Use shared dataset-2 to verify the completeness of the data by checking whether the timestamps for each unique (`id`, `id_2`) pair cover a full 24-hour and 7 days period

    Args:
        df (pandas.DataFrame)

    Returns:
        pd.Series: return a boolean series
    """
    df['start_timestamp'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'])
    df['end_timestamp'] = pd.to_datetime(df['endDay'] + ' ' + df['endTime'])


    duration = df.groupby(['id', 'id_2'])['end_timestamp', 'start_timestamp'].agg(
        {'end_timestamp': 'max', 'start_timestamp': 'min'})
    duration['time_diff'] = (duration['end_timestamp'] - duration['start_timestamp']).dt.total_seconds()


    full_24_hours = duration['time_diff'] >= 86400
    spans_7_days = duration.index.get_level_values('start_timestamp').dayofweek.nunique() == 7
    incorrect_timestamps = ~(full_24_hours & spans_7_days)

    return incorrect_timestamps


