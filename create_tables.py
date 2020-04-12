import configparser

import psycopg2

from sql_statements import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops tables using the queries defined in the
    drop_table_queries list if they exist"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates tables using the queries defined in the
    create_table_queries list if they don't yet exist"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read("settings.cfg")

    print("Connecting to Redshift cluster")
    conn = psycopg2.connect(
        f"""host={config.get("CLUSTER", "HOST")}
            dbname={config.get("CLUSTER", "DB_NAME")}
            user={config.get("CLUSTER", "DB_USER")}
            password={config.get("CLUSTER", "DB_PASSWORD")}
            port={config.get("CLUSTER", "DB_PORT")}"""
    )
    cur = conn.cursor()

    print("Dropping tables if they exist")
    drop_tables(cur, conn)

    print("Creating tables if they don't exist")
    create_tables(cur, conn)

    print("Done")
    conn.close()


if __name__ == "__main__":
    main()
