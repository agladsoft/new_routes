import sys
import numpy as np
import pandas as pd

types: dict = {
    'type_of_transportation': np.object,
    'text_route_number': np.object,
    'route_min_date': np.datetime64,
    'route_month': np.int64,
    'route_year': np.int64,
    'departure_station_code_of_rf': np.object,
    'destination_station_code_of_rf': np.object,
    'shipper_okpo': np.int64,
    'consignee_okpo': np.int64
}

dropped_columns: list = [
    'text_route_number_count',
    'route_month',
    'route_year',
    'departure_station_of_the_rf',
    'departure_region',
    'rf_destination_station',
    'destination_region',
    'shipper_by_puzt',
    'consignee_by_puzt',
    'teu'
]

dropped_columns2: list = [
    'type_of_transportation',
    'route_min_date',
    'departure_station_code_of_rf',
    'destination_station_code_of_rf',
    'payer_of_the_railway_tariff_unified',
    'shipper_okpo',
    'consignee_okpo'
]


class NewRoutes:
    def __init__(self, file_name, dir_name):
        self.file_name = file_name
        self.dir_name = dir_name

    def handle_rows(self):
        pass

    def read_df(self):
        df = pd.DataFrame(
            {
                "type_of_transportation":
                    ["Экспорт", "Импорт", "Импорт", "Импорт", "Экспорт", "Экспорт", "Экспорт", "Экспорт", "Импорт"],
                "text_route_number":
                    ["Маршрут_№_84", "Маршрут_№_4641", "Маршрут_№_4642", "Маршрут_№_4644", "Маршрут_№_4640",
                     "Маршрут_№_2352", "Маршрут_№_2399", "Маршрут_№_2347", "Маршрут_№_4643"],
                "text_route_number_count":
                    ['1', '1', '1', '1', '1', '1', '1', '1', '1'],
                "route_min_date":
                    ["2023-06-30 03:00:00", "2023-07-22 03:00:00", "2023-07-23 03:00:00", "2023-07-23 02:00:00",
                     "2023-07-25 03:00:00", "2023-08-30 03:00:00", "2023-09-22 03:00:00", "2023-10-22 03:00:00",
                     "2023-10-23 03:00:00"],
                "departure_station_code_of_rf":
                    ["01180", "01180", "11180", "11180", "98030", "98020", "98020", "01180", "12180"],
                "departure_station_of_the_rf":
                    ['Первая', 'Первая', 'Первая', 'Первая', 'Вторая', 'Вторая', 'Вторая', 'Первая', 'Третья'],
                "departure_region":
                    ['Первый', 'Первый', 'Первый', 'Первый', 'Второй', 'Второй', 'Второй', 'Первый', 'Третий'],
                "destination_station_code_of_rf":
                    ["03560", "03560", "14560", "13560", "19100", "19100", "19100", "03560", "14560"],
                "rf_destination_station":
                    ['Первая', 'Первая', 'Вторая', 'Третья', 'Четвертая', 'Четвертая', 'Четвертая', 'Первая', 'Вторая'],
                "destination_region":
                    ['Первый', 'Первый', 'Второй', 'Третий', 'Четвертый', 'Четвертый', 'Четвертый', 'Первый', 'Второй'],
                "payer_of_the_railway_tariff_unified":
                    ["ООО МОДУЛЬ", "ПАО ТРАНСКОНТЕЙНЕР", "ПАО ТРАНСКОНТЕЙНЕР 2", "ПАО ТРАНСКОНТЕЙНЕР 1", "ООО МОДУЛЬ",
                     "ООО ФЕСКО ИНТЕГРИРОВАННЫЙ ТРАНСПОРТ", "ООО МАКСИМА ЛОГИСТИК", "ПАО ТРАНСКОНТЕЙНЕР",
                     "ПАО ТРАНСКОНТЕЙНЕР 1"],
                "shipper_okpo":
                    ["000051321438", "000051321438", "000051321439", "000051321439", "000001126016", "000001126016",
                     "000001126016", "000051321438", "000051321439"],
                "shipper_by_puzt":
                    ['Один', 'Один', 'Два', 'Два', 'Три', 'Три', 'Три', 'Четыре', 'Пять'],
                "consignee_okpo":
                    ["000050011196", "000050011196", "000050011197", "000050011197", "000017652770", "000018011412",
                     "000017652770", "000050011196", "000050011197"],
                "consignee_by_puzt":
                    ['Один', 'Один', 'Два', 'Два', 'Три', 'Четыре', 'Три', 'Один', 'Два'],
                "teu":
                    ['10', '11', '12', '13', '14', '15', '16', '17', '18']
            }
        )
        df['route_min_date'] = df['route_min_date'].dt.date
        data = df.query('text_route_number_count == 1').drop(columns=dropped_columns)\
            .sort_values(by='route_min_date').reset_index(drop=True)

        data_old = data.drop(columns=dropped_columns2)
        data_old['old_text_route_number'] = np.nan
        data_old['сhanged_field'] = np.nan
        data_old['old_value_field'] = np.nan
        data_old = data_old.set_index('text_route_number', drop=False)

    def main(self):
        pass


NewRoutes(sys.argv[1], sys.argv[2]).main()
