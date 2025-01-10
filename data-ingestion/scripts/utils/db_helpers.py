import psycopg2

def get_db_connection(db_config):
    """
    Create a PostgreSQL database connection.
    """
    conn = psycopg2.connect(
        dbname=db_config['dbname'],
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port']
    )
    return conn