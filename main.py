import sys
import warnings
import numpy as np
import pandas as pd
from __init__ import *
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from datetime import datetime, timezone, date


pd.set_option('display.max_columns', 30)
pd.set_option("display.float_format", "{:.2f}".format)
pd.set_option('display.max_colwidth', 300)
warnings.simplefilter('ignore')

logger: logging.getLogger = get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))

table = 'new_route'

utc_current_datetime_start = datetime.now(timezone.utc)


def serialize_datetime(obj):
    if isinstance(obj, datetime) or isinstance(obj, date):
        return obj.isoformat()
    raise TypeError("Type not serializable")


def connect_to_db() -> Client:
    """
    Connecting to clickhouse.
    :return: Client ClickHouse.
    """
    try:
        client: Client = get_client(host=get_my_env_var('HOST'), database=get_my_env_var('DATABASE'),
                                    username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
        client.query("SET allow_experimental_lightweight_delete=1")
        logger.info("Success connect to clickhouse")
        new_routes = client.query("SELECT * FROM new_route_rf")
        # Чтобы проверить, есть ли данные. Так как переменная образуется, но внутри нее могут быть ошибки.
        print(new_routes.result_rows[0])
    except Exception as ex_connect:
        logger.error(f"Error connection to db {ex_connect}. Type error is {type(ex_connect)}.")
        sys.exit(1)
    return client, new_routes


client, new_routes = connect_to_db()

list_new_routes = []
for tuple_row in new_routes.result_rows:
    dict_new_routes = {}
    for column, row in zip(new_routes.column_names, tuple_row):
        dict_new_routes[column] = row
    list_new_routes.append(dict_new_routes)

client.query(f"DELETE FROM {table} WHERE uuid is not NULL")
logger.info("Successfully deleted new_route data")

df = pd.DataFrame(list_new_routes)

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

df.info()

# формируем рабочий датасет
df['route_min_date'] = pd.to_datetime(df['route_min_date'], errors='coerce').dt.date
data = df.query('text_route_number_count == 1').drop(columns=['text_route_number_count', 'route_month', 'route_year', 'departure_station_of_the_rf', 'departure_region', 'rf_destination_station', 'destination_region', 'shipper_by_puzt', 'consignee_by_puzt', 'teu']).sort_values(by='route_min_date').reset_index(drop=True)

# формируем датасет для сохранения данных
data_old = data.drop(columns=['type_of_transportation','route_min_date', 'departure_station_code_of_rf','destination_station_code_of_rf','payer_of_the_railway_tariff_unified','shipper_okpo','consignee_okpo'])
data_old['old_text_route_number'] = None
data_old['changed_field'] = None
data_old['old_value_field'] = None
data_old = data_old.set_index('text_route_number', drop=False)


row_columns = ['departure_station_code_of_rf','destination_station_code_of_rf','payer_of_the_railway_tariff_unified',
               'shipper_okpo','consignee_okpo']
for u in data.index:
    for i in reversed(range(u)):
        cout_true = 0
        seft = data.loc[u].compare(data.loc[i], keep_shape=True).T
        if (pd.isnull(seft['type_of_transportation']['self'])) and (pd.notnull(seft['route_min_date']['self'])):
            if (pd.isnull(seft['departure_station_code_of_rf']['self'])) and (pd.isnull(seft['destination_station_code_of_rf']['self'])):
                cout_true = seft.T['self'].isnull().sum()
        if cout_true == 5:
            for row_col in row_columns:
                if pd.notnull(seft[row_col]['self']):
                    data_old['old_text_route_number'][seft['text_route_number']['self']] = seft['text_route_number']['other']
                    data_old['changed_field'][seft['text_route_number']['self']] = row_col
            break
    else:
        for i in reversed(range(u)):
            cout_true = 0
            seft = data.loc[u].compare(data.loc[i], keep_shape=True).T
            if (pd.isnull(seft['type_of_transportation']['self'])) and (pd.notnull(seft['route_min_date']['self'])):
                if (pd.isnull(seft['departure_station_code_of_rf']['self'])) and (pd.isnull(seft['destination_station_code_of_rf']['self'])):
                    cout_true = seft.T['self'].isnull().sum()
            if cout_true == 4:
                changed_field = ''
                for row_col in row_columns:
                    if pd.notnull(seft[row_col]['self']):
                        changed_field += str(row_col) + ', '
                data_old['old_text_route_number'][seft['text_route_number']['self']] = seft['text_route_number']['other']
                data_old['changed_field'][seft['text_route_number']['self']] = changed_field[0 : -2]
                break
        else:
            for i in reversed(range(u)):
                cout_true = 0
                seft = data.loc[u].compare(data.loc[i], keep_shape=True).T
                if (pd.isnull(seft['type_of_transportation']['self'])) and (pd.notnull(seft['route_min_date']['self'])):
                    if (pd.notnull(seft['departure_station_code_of_rf']['self'])) or (pd.notnull(seft['destination_station_code_of_rf']['self'])):
                        cout_true = seft.T['self'].isnull().sum()
                if cout_true == 5:
                    for row_col in row_columns:
                        if pd.notnull(seft[row_col]['self']):
                            data_old['old_text_route_number'][seft['text_route_number']['self']] = seft['text_route_number']['other']
                            data_old['changed_field'][seft['text_route_number']['self']] = row_col
                    break
            else:
                for i in reversed(range(u)):
                    cout_true = 0
                    seft = data.loc[u].compare(data.loc[i], keep_shape=True).T
                    if (pd.isnull(seft['type_of_transportation']['self'])) and (pd.notnull(seft['route_min_date']['self'])):
                        if ((pd.notnull(seft['departure_station_code_of_rf']['self'])) and not (pd.notnull(seft['destination_station_code_of_rf']['self']))) or (not (pd.notnull(seft['departure_station_code_of_rf']['self'])) and (pd.notnull(seft['destination_station_code_of_rf']['self']))):
                            cout_true = seft.T['self'].isnull().sum()
                    if cout_true == 4:
                        changed_field = ''
                        for row_col in row_columns:
                            if pd.notnull(seft[row_col]['self']):
                                changed_field += str(row_col) + ', '
                        data_old['old_text_route_number'][seft['text_route_number']['self']] = seft['text_route_number']['other']
                        data_old['changed_field'][seft['text_route_number']['self']] = changed_field[0 : -2]
                        break
data_old = data_old.reset_index(drop=True)
df = df.merge(data_old, on='text_route_number', how='left')


df['category_route'] = np.nan
for u in df.index:
    old_value_field = ''
    if pd.notnull(df['old_text_route_number'][u]):
        seft = df.loc[u].compare(df.loc[int(str(df.index[(df['text_route_number'] == df['old_text_route_number'].loc[u]) & (df['text_route_number_count'] == 1)].tolist())[1 : -1])], keep_shape=True).T
        if pd.notnull(seft['departure_station_code_of_rf']['self']):
            old_value_field += str(seft['departure_station_of_the_rf']['other']) + ', '
        if pd.notnull(seft['destination_station_code_of_rf']['self']):
            old_value_field += str(seft['rf_destination_station']['other']) + ', '
        if pd.notnull(seft['payer_of_the_railway_tariff_unified']['self']):
            old_value_field += str(seft['payer_of_the_railway_tariff_unified']['other']) + ', '
        if pd.notnull(seft['shipper_okpo']['self']):
            old_value_field += str(seft['shipper_by_puzt']['other']) + ', '
        if pd.notnull(seft['consignee_okpo']['self']):
            old_value_field += str(seft['consignee_by_puzt']['other']) + ', '
    df['old_value_field'][u] = old_value_field[0 : -2]
    if (str(df['changed_field'][u]).find('departure_station_code_of_rf') != -1) or (str(df['changed_field'][u]).find('destination_station_code_of_rf') != -1) or pd.isnull(df['old_text_route_number'][u]):
        df['category_route'][u] = 'Новый маршрут'
    else:
        df['category_route'][u] = 'Изменение в маршруте'


df = df.replace({np.nan: None, "NaT": None})

try:
    client.insert_df(table=table, df=df)
    logger.info("Success insert to clickhouse")
except Exception as ex_insert:
    logger.error(f"Error insert to db {ex_insert}. Type error is {type(ex_insert)}.")
    sys.exit(1)

utc_current_datetime_finish = datetime.now(timezone.utc)
print(utc_current_datetime_start)
print(utc_current_datetime_finish)
