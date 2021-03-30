import os
import psycopg2
import yaml

from pyspark import SparkConf
from pyspark.sql import SparkSession
from transformer import transformer
# from utils.redshift_tables import make_tables


class ETLPipeline():
    """Extract-Transform-Load pipeline class.

    It uses Spark to handle the data, with the choice to write the output
    locally or to AWS Redshift
    """

    def __init__(self, cfg):
        """Initialize an ETLPipeline object.

        Parameters
        ----------
        cfg : string
            Configuration file
        """
        with open(cfg, 'r') as f:
            self.cfg = yaml.load(f, Loader=yaml.FullLoader)

    def initiate_spark(self):
        """Initialize the Spark instance.

        Returns
        -------
            Spark instance
        """
        spark = (
            SparkSession.builder.appName('ETL Pipeline')
            .config(conf=SparkConf())
            .enableHiveSupport()
            .getOrCreate()
            )
        return spark

    def connect_to_redshift(self):
        """Connect to AWS Redshift.

        Returns
        -------
            The Redshift connection and cursor
        """
        connection = psycopg2.connect(
            f'host={os.environ["REDSHIFT_HOST"]}'
            f'dbname={os.environ["REDSHIFT_DBNAME"]}'
            f'user={os.environ["REDSHIFT_USER"]}'
            f'password={os.environ["REDSHIFT_PW"]}'
            f'port={os.environ["REDSHIFT_PORT"]}'
            )
        cursor = connection.cursor()
        return connection, cursor

    def run(self):
        """Run the ETL pipeline."""
        spark = self.initiate_spark()

        # connection, cursor = self.connect_to_redshift()
        # make_tables(connection, cursor, self.cfg.get('sql'))

        transformer(spark, self.cfg.get('spark'), local_write=True)
        # Future note: Include the logic to load Spark dataframes into Redshift

        # connection.close()


if __name__ == '__main__':
    etl = ETLPipeline('config.yaml')
    etl.run()
