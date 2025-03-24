import os
import pytest
import pandas as pd
from pathlib import Path
from datetime import date
from typing import Optional
from unittest.mock import patch, MagicMock
from clickhouse_connect.driver.query import QueryResult

os.environ["XL_IDP_ROOT_NEW_ROUTE"] = os.path.dirname(os.path.dirname(__file__))

from scripts.main import RouteAnalyzer


@pytest.fixture
def mock_client(mocker: MagicMock):
    """Fixture to mock the ClickHouse client"""
    mocker.patch("scripts.main.get_client", return_value=MagicMock())


@pytest.fixture
def sample_data() -> pd.DataFrame:
    """
    Fixture to create a sample DataFrame.

    This DataFrame contains five rows with columns that match the expected
    columns returned from the ClickHouse query in `RouteAnalyzer.fetch_data`.

    The data is used in tests to simulate the data returned from the database.

    :return: A Pandas DataFrame object containing the sample data.
    """
    data: dict = {
        'text_route_number': ['М_1001', 'М_1002', 'М_1003', 'М_1004', 'М_1005'],
        'text_route_number_count': ['1', '1', '1', '1', '1'],
        'route_month': [1, 2, 3, 4, 5],
        'route_year': [2024, 2024, 2024, 2024, 2024],
        'teu': [10, 20, 30, 40, 50],
        'route_min_date': ['2024-01-01', '2024-02-01', '2024-03-01', '2024-03-02', '2024-03-03'],
        'departure_station_code_of_rf': ['001', '002', '001', '001', '005'],
        'destination_station_code_of_rf': ['100', '200', '100', '100', '500'],
        'payer_of_the_railway_tariff_unified': ['PAYER1', 'PAYER2', 'PAYER3', 'PAYER1', 'PAYER5'],
        'shipper_okpo': ['SHIP1', 'SHIP2', 'SHIP1', 'SHIP1', 'SHIP5'],
        'consignee_okpo': ['CONS1', 'CONS2', 'CONS1', 'CONS4', 'CONS5'],
        'departure_station_of_the_rf': ['001', '002', '001', '001', '005'],
        'rf_destination_station': ['100', '200', '100', '100', '500'],
        'shipper_by_puzt': ['SHIP1', 'SHIP2', 'SHIP1', 'SHIP1', 'SHIP5'],
        'consignee_by_puzt': ['CONS1', 'CONS2', 'CONS1', 'CONS4', 'CONS5'],
        'type_of_transportation': ['TYPE1', 'TYPE2', 'TYPE1', 'TYPE1', 'TYPE5']
    }
    return pd.DataFrame(data)


@pytest.fixture
def analyzer(mock_client: MagicMock) -> RouteAnalyzer:
    """
    Fixture to create a RouteAnalyzer instance with a mocked client
    :param mock_client: Mock client.
    :return: RouteAnalyzer
    """
    return RouteAnalyzer()


def test_init(analyzer: RouteAnalyzer) -> None:
    """
    Test the __init__ method of RouteAnalyzer.

    This test verifies that the RouteAnalyzer instance is initialized
    with the correct default values for its attributes, including the
    table name, client connection, and the lengths of key_columns and
    old_value_mapping lists.

    :param analyzer: RouteAnalyzer instance with a mocked client.
    """
    assert analyzer.table == 'new_route'
    assert analyzer.client is not None
    assert len(analyzer.key_columns) == 5
    assert len(analyzer.old_value_mapping) == 5


@patch('scripts.main.get_client')
def test_connect_to_db_failure(mock_client: MagicMock) -> None:
    """
    Test the connect_to_db method of RouteAnalyzer when it fails to connect to ClickHouse.

    This test verifies that the RouteAnalyzer instance will raise a SystemExit
    when it fails to connect to ClickHouse.

    :param mock_client: Mock client.
    """
    mock_client.side_effect = Exception("Connection error")

    with pytest.raises(SystemExit):
        RouteAnalyzer()


def test_prepare_data(sample_data: pd.DataFrame, analyzer: RouteAnalyzer) -> None:
    """
    Test the prepare_data method of RouteAnalyzer.

    This test verifies that the prepare_data method correctly performs the following operations:
    - Casts certain columns to Int64.
    - Converts the 'route_min_date' column to a datetime object.
    - Filters the dataframe to only include rows with 'text_route_number_count' equal to 1.
    - Sorts the dataframe by 'route_min_date'.
    - Resets the index of the dataframe.
    - Limits the dataframe to the first 138 rows.
    - Adds three new columns: 'category_route', 'old_text_route_number', 'changed_field', and 'old_value_field'.
      These columns are initialized to 'Новый маршрут', None, None, and None, respectively.

    :param sample_data: Pandas DataFrame object.
    :param analyzer: RouteAnalyzer instance with a mocked client.
    """
    result: pd.DataFrame = analyzer.prepare_data(sample_data)

    # Check column types
    assert pd.api.types.is_integer_dtype(result['text_route_number_count'])
    assert pd.api.types.is_integer_dtype(result['route_month'])
    assert pd.api.types.is_integer_dtype(result['route_year'])
    assert pd.api.types.is_integer_dtype(result['teu'])

    # Check route_min_date conversion
    assert isinstance(result['route_min_date'].iloc[0], date)

    # Check new columns
    assert 'category_route' in result.columns
    assert 'old_text_route_number' in result.columns
    assert 'changed_field' in result.columns
    assert 'old_value_field' in result.columns

    # Check default values
    assert result['category_route'].iloc[0] == 'Новый маршрут'
    assert pd.isna(result['old_text_route_number'].iloc[0])
    assert pd.isna(result['changed_field'].iloc[0])
    assert pd.isna(result['old_value_field'].iloc[0])


def test_analyze_routes(sample_data: pd.DataFrame, analyzer: RouteAnalyzer) -> None:
    """
    Test the analyze_routes method of RouteAnalyzer.

    This test verifies that the analyze_routes method correctly identifies changes
    in routes and marks them as 'Изменение в маршруте'.

    The test uses the sample_data DataFrame fixture to test the method.

    :param sample_data: Pandas DataFrame object containing sample data.
    :param analyzer: RouteAnalyzer instance with a mocked client.
    """
    prepared_data: pd.DataFrame = analyzer.prepare_data(sample_data)
    result: pd.DataFrame = analyzer.analyze_routes(prepared_data)

    # Routes 3 and 4 should be marked as changes because they have similar key parameters to route 1
    assert result['category_route'].iloc[0] == 'Новый маршрут'
    assert result['category_route'].iloc[1] == 'Новый маршрут'
    assert result['category_route'].iloc[2] == 'Изменение в маршруте'
    assert result['category_route'].iloc[3] == 'Изменение в маршруте'
    assert result['category_route'].iloc[4] == 'Новый маршрут'


def test_compare_and_update_routes(sample_data: pd.DataFrame, analyzer: RouteAnalyzer) -> None:
    """
    Test the compare_and_update_routes method of RouteAnalyzer.
    
    This test verifies that the compare_and_update_routes method correctly compares two routes
    based on key columns and route_min_date, and updates the 'old_text_route_number' column
    if a match is found.
    
    The test uses the sample_data DataFrame fixture to test the method.
    
    :param sample_data: Pandas DataFrame object containing sample data.
    :param analyzer: RouteAnalyzer instance with a mocked client.
    """
    prepared_data: pd.DataFrame = analyzer.prepare_data(sample_data)

    # Compare route 3 (index 2) with route 1 (index 0) - should match
    result: bool = analyzer.compare_and_update_routes(prepared_data, 2, 0)
    assert result
    assert prepared_data.loc[2, 'old_text_route_number'] == 'М_1001'

    # Compare route 2 (index 1) with route 1 (index 0) - should not match
    result = analyzer.compare_and_update_routes(prepared_data, 1, 0)
    assert result == False

    # Compare route 5 (index 4) with route 1 (index 0) - should not match
    result = analyzer.compare_and_update_routes(prepared_data, 4, 0)
    assert result == False

    # Compare route with same date - should not match
    prepared_data.loc[1, 'route_min_date'] = prepared_data.loc[0, 'route_min_date']
    result = analyzer.compare_and_update_routes(prepared_data, 1, 0)
    assert result == False


def test_save_to_file(sample_data: pd.DataFrame, analyzer: RouteAnalyzer, tmp_path: Path) -> None:
    """
    Test the save_to_file method of RouteAnalyzer.

    This test verifies that the save_to_file method correctly saves the DataFrame to a CSV file.

    The test uses a temporary directory to store the output file.

    :param sample_data: Pandas DataFrame object containing sample data.
    :param analyzer: RouteAnalyzer instance with a mocked client.
    :param tmp_path: Path to a temporary directory.
        """
    filepath: Path = tmp_path / "test_output.csv"
    analyzer.save_to_file(sample_data, str(filepath))
    assert filepath.exists()

    # Read back the file to verify
    df_read: pd.DataFrame = pd.read_csv(filepath)
    assert len(df_read) == len(sample_data)
    assert list(df_read.columns) == list(sample_data.columns)


@patch('pandas.DataFrame.to_csv')
def test_save_to_file_error(mock_to_csv: MagicMock, sample_data: pd.DataFrame, analyzer: RouteAnalyzer) -> None:
    """
    Test the save_to_file method with an error.

    This test verifies that the save_to_file method logs an error if an exception
    occurs while attempting to save the DataFrame to a CSV file, but does not
    raise an exception.

    The test uses the mock_to_csv fixture to mock the to_csv method of the
    DataFrame, and sets its side_effect to raise an exception when called.
    The test then calls the save_to_file method with a sample DataFrame and
    verifies that the method does not raise an exception.

    :param mock_to_csv: A MagicMock object that mocks the to_csv method of the DataFrame.
    :param sample_data: A Pandas DataFrame object containing sample data.
    :param analyzer: A RouteAnalyzer instance with a mocked client.
    """
    mock_to_csv.side_effect = Exception("Write error")
    analyzer.save_to_file(sample_data)  # Should log error but not raise exception


def test_fetch_data_success(analyzer: RouteAnalyzer) -> None:
    """
    Test the fetch_data method when data is found in ClickHouse.

    This test verifies that the fetch_data method correctly converts the
    result from ClickHouse to a Pandas DataFrame.

    The test creates a mock QueryResult object with a single row containing
    the column names and a single row of data. The fetch_data method is
    then called and the result is verified to be a Pandas DataFrame with
    the correct column names and values.

    :param analyzer: A RouteAnalyzer instance with a mocked client.
    """
    mock_result: MagicMock = MagicMock(spec=QueryResult)
    mock_result.result_rows = [['М_1001', '1', 1, 2024, 10, '2024-01-01']]
    mock_result.column_names = [
        'text_route_number',
        'text_route_number_count',
        'route_month',
        'route_year',
        'teu',
        'route_min_date'
    ]
    analyzer.client.query.return_value = mock_result

    result: pd.DataFrame = analyzer.fetch_data()
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1


def test_fetch_data_no_data(analyzer: RouteAnalyzer):
    """
    Test the fetch_data method when no data is found in ClickHouse.

    This test verifies that the fetch_data method returns None when the
    query result from ClickHouse is empty. A mock QueryResult object with
    no rows is used to simulate an empty result set.

    :param analyzer: A RouteAnalyzer instance with a mocked client.
    """
    mock_result: MagicMock = MagicMock(spec=QueryResult)
    mock_result.result_rows = []
    mock_result.column_names = [
        'text_route_number',
        'text_route_number_count',
        'route_month',
        'route_year',
        'teu',
        'route_min_date'
    ]
    analyzer.client.query.return_value = mock_result

    result: Optional[pd.DataFrame] = analyzer.fetch_data()
    assert result is None


@patch('scripts.main.RouteAnalyzer.fetch_data')
@patch('scripts.main.RouteAnalyzer.prepare_data')
@patch('scripts.main.RouteAnalyzer.analyze_routes')
@patch('scripts.main.RouteAnalyzer.insert_data')
def test_run_with_data(
    mock_insert: MagicMock,
    mock_analyze: MagicMock,
    mock_prepare: MagicMock,
    mock_fetch: MagicMock,
    analyzer: RouteAnalyzer
) -> None:
    """
    Test the run method when data is successfully fetched from ClickHouse.

    This test verifies that the run method correctly calls the fetch_data,
    prepare_data, analyze_routes, and insert_data methods in sequence when
    data is successfully fetched. The mocked methods are used to simulate
    the process and ensure that each method is called with the appropriate
    arguments.

    :param mock_insert: Mock for the insert_data method.
    :param mock_analyze: Mock for the analyze_routes method.
    :param mock_prepare: Mock for the prepare_data method.
    :param mock_fetch: Mock for the fetch_data method.
    :param analyzer: A RouteAnalyzer instance with a mocked client.
    """
    mock_df: pd.DataFrame = pd.DataFrame({'test': [1, 2, 3]})
    mock_fetch.return_value = mock_df
    mock_prepare.return_value = mock_df
    mock_analyze.return_value = mock_df

    analyzer.run()

    mock_fetch.assert_called_once()
    mock_prepare.assert_called_once_with(mock_df)
    mock_analyze.assert_called_once_with(mock_df)
    mock_insert.assert_called_once_with(mock_df)


@patch('scripts.main.RouteAnalyzer.fetch_data')
@patch('scripts.main.RouteAnalyzer.prepare_data')
@patch('scripts.main.RouteAnalyzer.analyze_routes')
@patch('scripts.main.RouteAnalyzer.insert_data')
def test_run_no_data(
    mock_insert: MagicMock,
    mock_analyze: MagicMock,
    mock_prepare: MagicMock,
    mock_fetch: MagicMock,
    analyzer: RouteAnalyzer
) -> None:
    """
    Test the run method when no data is fetched from ClickHouse.

    This test ensures that when the fetch_data method returns None,
    the run method does not proceed to call the prepare_data,
    analyze_routes, or insert_data methods.

    :param mock_insert: Mock for the insert_data method.
    :param mock_analyze: Mock for the analyze_routes method.
    :param mock_prepare: Mock for the prepare_data method.
    :param mock_fetch: Mock for the fetch_data method.
    :param analyzer: A RouteAnalyzer instance with a mocked client.
    """
    mock_fetch.return_value = None

    analyzer.run()

    mock_fetch.assert_called_once()
    mock_prepare.assert_not_called()
    mock_analyze.assert_not_called()
    mock_insert.assert_not_called()


@patch('scripts.main.RouteAnalyzer.save_to_file')
def test_insert_data(mock_save: MagicMock, analyzer: RouteAnalyzer, sample_data: pd.DataFrame) -> None:
    """
    Test the insert_data method.

    This test verifies that the insert_data method calls the save_to_file method
    with the given DataFrame as an argument.

    The test uses a mock for the save_to_file method to ensure that it is called
    with the correct argument.

    :param mock_save: Mock for the save_to_file method.
    :param analyzer: A RouteAnalyzer instance with a mocked client.
    :param sample_data: A Pandas DataFrame object containing sample data.
    """
    analyzer.insert_data(sample_data)

    mock_save.assert_called_once()