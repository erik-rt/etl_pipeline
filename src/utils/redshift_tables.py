import utils.sql
import yaml


def redshift_tables(cursor, connection, config):
    # Load the Redshift tables from the configuration file
    tables = yaml.load(config, yaml.FullLoader).get('tables')

    for table in tables:
        # Drop the table if it exists for each Redshift table
        # ! Should this be with os.path.join or the .join str method?
        ' '.join(sql.drop_query, table)
