import os
import psycopg2
import yaml

from pyspark.sql import SparkSession
from utils.redshift_tables import make_tables

class ETLPipeline():
    def __init__(self):
        pass

    def initiate_spark(self):
        spark = SparkSession.builder.appName()
    def connect_to_redshift(self, connection):
        connection = psycopg2.connect(
            f'host={os.environ['REDSHIFT_HOST']}'
            f'dbname={os.environ['REDSHIFT_DBNAME']}'
            f'user={os.environ['REDSHIFT_USER']}'
            f'password={os.environ['REDSHIFT_PW']}'
            f'port={os.environ['REDSHIFT_PORT']}'
            )
        cursor = connection.cursor()

    def main():
        connection = psycopg2.connect(
            f'host={os.environ['REDSHIFT_HOST']}'
            f'dbname={os.environ['REDSHIFT_DBNAME']}'
            f'user={os.environ['REDSHIFT_USER']}'
            f'password={os.environ['REDSHIFT_PW']}'
            f'port={os.environ['REDSHIFT_PORT']}'
            )
        cursor = connection.cursor()
        cfg = 'config.yaml'

        make_tables(connection, cursor, cfg)
        
        connection.close()

if __name__ == '__main__':
    main()
