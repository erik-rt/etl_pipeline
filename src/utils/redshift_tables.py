from utils.sql import sql


def make_tables(connection, cursor, cfg):
    """Create the tables in Redshift.

    Parameters
    ----------
    connection :
        Redshift connection with Psycopg2
    cursor :
        Psycopg2 cursor
    cfg :
        Configuration file
    """
    # Load the Redshift tables from the configuration file
    tables = cfg.get('tables')

    for table in tables:
        # Drop the table if it exists for each Redshift table
        ' '.join(sql.drop_query, table)

        # Create the table if it does not exist
        ' '.join(sql.create_query, table, sql.table_attributes[table])
