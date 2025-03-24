import sys
import pandas as pd
from __init__ import *
from typing import Optional
from datetime import datetime
from typing import List, Union
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.query import QueryResult


logger: logging.getLogger = get_logger(
    str(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))
)


class RouteAnalyzer:
    def __init__(self):
        self.table: str = 'new_route'
        self.client: Client = self.connect_to_db()
        self.key_columns: list = [
            'departure_station_code_of_rf', 'destination_station_code_of_rf',
            'payer_of_the_railway_tariff_unified', 'shipper_okpo', 'consignee_okpo'
        ]
        self.old_value_mapping: dict = {
            'departure_station_code_of_rf': 'departure_station_of_the_rf',
            'destination_station_code_of_rf': 'rf_destination_station',
            'payer_of_the_railway_tariff_unified': 'payer_of_the_railway_tariff_unified',
            'shipper_okpo': 'shipper_by_puzt',
            'consignee_okpo': 'consignee_by_puzt'
        }

    def connect_to_db(self) -> Client:
        """
        Connecting to clickhouse.

        :return: Client ClickHouse.
        """
        try:
            client: Client = get_client(
                host=get_my_env_var('HOST'),
                database=get_my_env_var('DATABASE'),
                username=get_my_env_var('USERNAME_DB'),
                password=get_my_env_var('PASSWORD')
            )
            logger.info("Success connected to clickhouse")
            client.query("SET allow_experimental_lightweight_delete=1")
            client.query(f"DELETE FROM {self.table} WHERE uuid is not NULL")
            logger.info(f"Success deleted data from {self.table}")
        except Exception as ex_connect:
            logger.error(f"Error connection to db {ex_connect}. Type error is {type(ex_connect)}.")
            sys.exit(1)
        return client

    def run(self) -> None:
        """
        Executes the route analysis process.

        This method fetches data from ClickHouse, prepares it for analysis,
        analyzes routes and identifies changes, and then inserts the analyzed
        data into ClickHouse.

        :return: None
        """
        logger.info("Starting route analysis process.")
        df: Optional[pd.DataFrame] = self.fetch_data()
        if df is not None:
            logger.info(f"Fetched {len(df)} rows of data.")
            df = self.prepare_data(df)
            df = self.analyze_routes(df)
            self.insert_data(df)
            logger.info("Route analysis completed and data inserted.")

    def fetch_data(self) -> Optional[pd.DataFrame]:
        """
        Fetches data from ClickHouse and converts it to a DataFrame.

        This method attempts to retrieve all data from the 'new_route_rf' table in ClickHouse.
        If the query is successful and data is found, it is converted to a Pandas DataFrame object.
        If no data is found, a warning is logged and `None` is returned.

        :return: A Pandas DataFrame object containing the data from ClickHouse, or `None` if no data is found.
        :raises SystemExit: If an error occurs while connecting to ClickHouse.
        """
        try:
            logger.info("Fetching data from ClickHouse...")
            new_routes: QueryResult = self.client.query(
                "SELECT * FROM new_route_rf WHERE text_route_number_count == '1'"
            )
            if new_routes.result_rows:
                return pd.DataFrame(new_routes.result_rows, columns=new_routes.column_names)  # type: ignore
            logger.warning("No data found in new_route_rf")
            return None
        except Exception as ex_connect:
            logger.error(f"Error fetching data from ClickHouse: {ex_connect}")
            sys.exit(1)

    @staticmethod
    def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares the dataframe for analysis.

        This method performs the following steps:
        - Casts certain columns to Int64.
        - Converts the 'route_min_date' column to a datetime object.
        - Filters the dataframe to only include rows with 'text_route_number_count' equal to 1.
        - Sorts the dataframe by 'route_min_date'.
        - Resets the index of the dataframe.
        - Limits the dataframe to the first 138 rows.
        - Adds three new columns: 'category_route', 'old_text_route_number', 'changed_field', and 'old_value_field'.
          These columns are initialized to 'Новый маршрут', None, None, and None, respectively.

        :param df: A Pandas DataFrame object.
        :return: A Pandas DataFrame object with the transformations applied.
        """
        logger.info("Preparing data for analysis...")
        df[[
            'text_route_number_count',
            'route_month',
            'route_year',
            'teu'
        ]] = df[[
            'text_route_number_count',
            'route_month',
            'route_year',
            'teu'
        ]].astype('Int64')

        df['route_min_date'] = pd.to_datetime(df['route_min_date'], errors='coerce').dt.date
        df = (
            df.assign(text_route_number_int=df['text_route_number'].str.replace('М_', '', regex=True).astype('uint32'))
            .sort_values(by=['route_min_date', 'text_route_number_int'])
            .drop(columns=['text_route_number_int'])
            .reset_index(drop=True)
        )
        df['category_route'] = 'Новый маршрут'  # Default category
        df['old_text_route_number'] = None
        df['changed_field'] = None
        df['old_value_field'] = None
        logger.info(f"Prepared {len(df)} rows of data.")
        return df

    def analyze_routes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyzes routes and identifies changes.

        This method iterates over the rows of the dataframe, comparing each row with all previous rows.
        If a match is found, the category of the current row is set to 'Изменение в маршруте'.
        The method returns the modified dataframe.

        :param df: A Pandas DataFrame object.
        :return: A Pandas DataFrame object with the 'category_route' column updated.
        """
        logger.info("Analyzing routes for changes...")
        for i in range(1, len(df)):
            for j in reversed(range(i)):
                if self.compare_and_update_routes(df, i, j):
                    df.loc[i, 'category_route'] = 'Изменение в маршруте'
                    logger.debug(f"Route at index {i} marked as 'Изменение в маршруте'")
                    break
        logger.info("Route analysis completed.")
        return df

    def compare_and_update_routes(self, df: pd.DataFrame, current_index: int, previous_index: int) -> bool:
        """
        Compares two routes based on key columns and route_min_date.

        This method is used in the analyze_routes method to compare a current route with all previous routes.
        The method takes a Pandas DataFrame object and two indices as parameters.
        It returns True if the current route matches the previous route, False otherwise.

        The comparison is done on the following columns: route_min_date, type_of_transportation,
        departure_station_code_of_rf, destination_station_code_of_rf, payer_of_the_railway_tariff_unified,
        shipper_okpo, consignee_okpo.

        The following conditions must be met for the method to return True:
        - The `route_min_date` values **must be different** for the current and previous routes.
        - The `type_of_transportation` must be the same.
        - At least `MIN_MATCHING_KEY_THRESHOLD` key columns must match.
        - If the `old_text_route_number` column is empty, it will be populated with the previous route's `text_route_number`,
          and the `changed_field` and `old_value_field` columns will store the changed fields and their old values.
        - The `departure_station_code_of_rf` and `destination_station_code_of_rf` must match.

        :param df: A Pandas DataFrame object.
        :param current_index: An integer index of the current row in the dataframe.
        :param previous_index: An integer index of the previous row in the dataframe.
        :return: A boolean indicating whether the current route matches the previous route.
        """
        current_route: pd.Series = df.iloc[current_index]
        previous_route: pd.Series = df.iloc[previous_index]

        if current_route['route_min_date'] == previous_route['route_min_date']:
            return False
        if current_route['type_of_transportation'] != previous_route['type_of_transportation']:
            return False

        matching_values: pd.Series = pd.Series(
            current_route[self.key_columns] == previous_route[self.key_columns]
        ).astype(int)
        if matching_values.sum() < MIN_MATCHING_KEY_THRESHOLD:
            return False

        # Проверяем только old_text_route_number, если оно пустое, заполняем и обновляем остальные поля
        if pd.isna(df.loc[current_index, 'old_text_route_number']):
            changed_fields: Union[List[int], list] = matching_values[matching_values == 0].index.tolist()
            df.loc[current_index, 'old_text_route_number'] = previous_route['text_route_number']
            df.loc[current_index, 'changed_field'] = ', '.join(changed_fields)

            old_values: List[str] = [
                f"{previous_route[self.old_value_mapping.get(field, field)]}" for field in changed_fields
            ]
            df.loc[current_index, 'old_value_field'] = ', '.join(old_values)

        return (
            matching_values['departure_station_code_of_rf'] == 1
            and matching_values['destination_station_code_of_rf'] == 1
        )

    @staticmethod
    def save_to_file(df: pd.DataFrame, filename: str = 'output.csv'):
        """
        Saves the DataFrame to a CSV file.

        This method takes a Pandas DataFrame object and an optional filename as parameters.
        It attempts to save the dataframe to the specified filename.
        If the save is successful, a success message is logged.
        If an error occurs, an error message is logged.

        :param df: A Pandas DataFrame object.
        :param filename: A string filename to save the dataframe to. Defaults to 'output.csv'.
        """
        try:
            df.to_csv(filename, index=False)
            logger.info(f"DataFrame saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving DataFrame to file: {e}")

    def insert_data(self, df: pd.DataFrame):
        """
        Inserts the analyzed data into ClickHouse.

        This method takes a Pandas DataFrame object as a parameter.
        It replaces any NaN or NaT values in the dataframe with None,
        saves the dataframe to a CSV file, and then attempts to insert
        the dataframe into ClickHouse.
        If the insertion is successful, a success message is logged.
        If an error occurs, an error message is logged and the program exits.

        :param df: A Pandas DataFrame object.
        """
        logger.info("Inserting data into ClickHouse...")
        df: pd.DataFrame = df.replace({pd.NA: None, "NaT": None})
        self.save_to_file(df)  # Save before attempting insertion
        try:
            self.client.insert_df(table=self.table, df=df)
            logger.info(f"Data successfully inserted into {self.table}. Number of rows: {len(df)}")
        except Exception as ex_insert:
            logger.error(f"Error inserting data into {self.table}: {ex_insert}")
            sys.exit(1)


if __name__ == '__main__':
    analyzer = RouteAnalyzer()
    analyzer.run()
