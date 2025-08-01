import time
import os

def read_data_from_sources():
    from cores.files import read_from_csv
    from cores.mysql import mysql_connection, read_from_mysql
    from cores.api import read_from_api

    # MySQL Creds
    host = os.getenv('MYSQL_HOSTNAME')
    port = int(os.getenv('MYSQL_PORT'))
    user = os.getenv('MYSQL_USERNAME')
    password = os.getenv('MYSQL_PASSWORD')
    database = os.getenv('MYSQL_DATABASE')

    start_date = os.getenv('API_PARAM_START_DATE')
    end_date = os.getenv('API_PARAM_END_DATE')
    api = f"https://api.open-meteo.com/v1/forecast?latitude=2.0167&longitude=117.3000&daily=temperature_2m_mean,precipitation_sum&timezone=Asia/Jakarta&past_days=0&start_date={start_date}&end_date={end_date}"

    root_dir = os.getenv('PROJECT_DATA_DIR')

    # Read csv [PASS]
    equipement_sensor = read_from_csv(f"{root_dir}/equipment_sensors.csv")
    # Read from mysql [PASS]
    connection = mysql_connection(host=host, port=port, user=user, password=password, database=database)
    production_logs = read_from_mysql(connection, "select * from production_logs")
    connection.close()
    # Read api [PASS]
    daily_weather = [read_from_api(api)]

    return {
        "file_csv_equipement_sensor": equipement_sensor, 
        "mysql_coal_mining_production_logs": production_logs, 
        "api_daily_weather": daily_weather
    }

def inject_data_into_dwh(data: dict):
    from cores.clickhouse import clickhouse_connection, create_table_clickhouse, insert_rows_clickhouse

    # Clickhouse Creds
    host = os.getenv('CLICKHOUSE_HOSTNAME')
    port = int(os.getenv('CLICKHOUSE_PORT'))
    user = os.getenv('CLICKHOUSE_USERNAME')
    password = os.getenv('CLICKHOUSE_PASSWORD')
    database = 'layer_bronze'

    print(user, password, host)

    ch_connection = clickhouse_connection(host=host, port=port, user=user, password=password, database=database)
    
    for table_name in data.keys():
        # Create table with schema based on extracted data
        create_table_clickhouse(
            source_data=data[table_name][0],
            destination_connect=ch_connection,
            destination_table_name=table_name,
            if_exists='replace'
        )
        # Insert data into table
        insert_rows_clickhouse(ch_connection, table_name, data[table_name])
    
    ch_connection.close()
    

def generate_summary_coal_mining():
    from cores.clickhouse import clickhouse_connection, clickhouse_execute

    host = os.getenv('CLICKHOUSE_HOSTNAME')
    port = int(os.getenv('CLICKHOUSE_PORT'))
    user = os.getenv('CLICKHOUSE_USERNAME')
    password = os.getenv('CLICKHOUSE_PASSWORD')
    database = 'layer_gold'

    print(user, password, host)

    root_dir = os.getenv('PROJECT_QUERIES_DIR')
    queries_path = os.getenv('QUERIES_FILENAME').split(',')
    
    ch_connection = clickhouse_connection(host=host, port=port, user=user, password=password, database=database)
    
    # Read query from path given
    for query_path in queries_path:
        with open(f'{root_dir}/{query_path}', 'r') as file:
            f = file.read()
        # Exec multi statement
        statements = [x for x in f.split(';') if x]
        for statement in statements:
            clickhouse_execute(ch_connection, statement)

    ch_connection.close()

if __name__ == "__main__":
    time.sleep(15) 
    data = read_data_from_sources()
    inject_data_into_dwh(data=data)
    generate_summary_coal_mining()