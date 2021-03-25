from utils.sql import sql
import yaml


def make_tables(connection, cursor, config):
    # Load the Redshift tables from the configuration file
    tables = yaml.load(config, Loader=yaml.FullLoader).get('tables')

    for table in tables:
        # Drop the table if it exists for each Redshift table
        # ! Should this be with os.path.join or the .join str method?
        ' '.join(sql.drop_query, table)

        # Create the table if it does not exist
        ' '.join(sql.create_query, table, sql.table_attributes[table])

    # TODO: Add functionality for copying and inserting content into tables
