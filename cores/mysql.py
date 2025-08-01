import pymysql
from pymysql.constants import CLIENT

def mysql_connection(host: str, port: int, user: str, password: str, database: str):
    """
    Create connection to mysql database.

    Arg(s):
        host: hostname or ip address database server
        port: port database server
        user: user name to login database server
        pass: password to login database server
        database: database name that will connect

    Return(s):
        mysql connection
    """
    client = pymysql.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        client_flag=CLIENT.MULTI_STATEMENTS,
        autocommit=True
    )
    return client

def read_from_mysql(connection: mysql_connection, query: str):
    """
    Read data from mysql database.

    Arg(s):
        connection: connection to destination database
        query: query to get data. use select query

    Return(s):
        dictionary with format:
            [{
                'key': 'value', 
                'key': {'key': 'value'}
            }]
    """
    print(f"INF read data from query below: \n{query}")
    destination_cursor = connection.cursor()
    destination_cursor.execute(query)
    columns = [desc[0] for desc in destination_cursor.description]
    values = destination_cursor.fetchall()
    result = [dict(zip(columns, x)) for x in values]
    return result