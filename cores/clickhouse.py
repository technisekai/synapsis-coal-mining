import clickhouse_connect
from typing import Literal
import json

def clickhouse_connection(host: str, port: int, user: str, password: str, database: str):
    """
    Create connection to clickhouse database.

    Arg(s):
        host: hostname or ip address database server
        port: port database server
        user: user name to login database server
        pass: password to login database server
        database: database name that will connect

    Return(s):
        clickhouse connection
    """
    conn = clickhouse_connect.get_client(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    return conn

def clickhouse_execute(conn, query: str):
    conn.command(query)

def convert_to_clickhouse_schema(source_schema: dict):
    """
    Convert json key-value to clickhouse schema.

    Arg(s):
        source_schema: clickhouse data {'key': 'value'}
    
    Return(s):
        dict of postgres schema:
            {
                'column name': 'column data type'
            }
    """
    clickhouse_schema = {}
    # Mapping dict data type to postgre data type
    for key in source_schema.keys():
        json_type_name = (type(source_schema[key]).__name__).lower()
        if json_type_name in ['str', 'nonetype']:
            clickhouse_schema[key] = 'Nullable(varchar)'
        elif json_type_name == 'int':
            clickhouse_schema[key] = 'Nullable(Int16)'
        elif json_type_name in ['float', 'decimal']:
            clickhouse_schema[key] = 'Nullable(Float64)'
        elif json_type_name == 'bool':
            clickhouse_schema[key] = 'Nullable(Bool)'
        elif json_type_name == 'date':
            clickhouse_schema[key] = 'Nullable(Date)'
        elif json_type_name == 'datetime':
            clickhouse_schema[key] = 'Nullable(DateTime)'
        elif json_type_name in ['dict', 'list']:
            clickhouse_schema[key] = 'varchar'
        else:
            raise Exception(f'Err {json_type_name} data type not defined!')
    print(f"INF successfully generated clickhouse schema")
    return clickhouse_schema

def create_table_clickhouse(
    source_data: dict, 
    destination_table_name: str, 
    destination_connect: clickhouse_connection,
    if_exists: Literal['ignore', 'replace'] = 'ignore'
):
    """
    Create table in clickhouse database.
    
    Arg(s):
        destination_connect: connection to destination database
        destination_table_name: table destination to inject data
        source_data: data to inject
        if_exists:
            ignore: if table exists, dont do anything
            replace: create new table although it's exists
    
    Return(s):
        None, table created
    """
    clickhouse_schema = convert_to_clickhouse_schema(source_data)
    if if_exists == 'replace':
        print(f"INF drop table if exists {destination_table_name}")
        query_drop_table = f"""
        drop table if exists {destination_table_name};
        """
        clickhouse_execute(destination_connect, query_drop_table)
    query_create_table = f"""
    create table if not exists {destination_table_name} (
        _id UUID DEFAULT generateUUIDv4(),
        {', '.join([' '.join([f'"{x[0]}"', x[1]]) for x in zip(clickhouse_schema.keys(), clickhouse_schema.values())])},
        _created_at DateTime DEFAULT now(),
        _updated_at DateTime DEFAULT now()
    ) ENGINE = MergeTree ORDER BY _id;
    """
    print(f"INF create table if not exists {destination_table_name}")
    clickhouse_execute(destination_connect, query_create_table)

def insert_rows_clickhouse(destination_connect:clickhouse_connection, destination_table_name: str, data: list[dict]):
    columns = list(data[0].keys())
    values = [tuple(row[col] \
                    if not isinstance(row.get(col, None), (dict, list)) \
                        else json.dumps(row[col]) \
                            for col in columns) for row in data]
    
    print(values)
    destination_connect.insert(
        table=destination_table_name, 
        column_names=columns, 
        data=values
    )
