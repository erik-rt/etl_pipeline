import os
import psycopg2

from pyspark import SparkConf
from pyspark.sql import SparkSession
from utils.redshift_tables import make_tables


class ETLPipeline():
    def __init__(self):
        pass

    def initiate_spark(self):
        spark = (
            SparkSession.builder.appName('ETL Pipeline')
            .builder.config(conf=SparkConf())
            .enableHiveSupport()
            .getOrCreate()
            )
        return spark

    def connect_to_redshift(self):
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
        connection, cursor = self.connect_to_redshift()
        make_tables(connection, cursor, config='config.yaml')

        connection.close()


if __name__ == '__main__':
    ETLPipeline.run()
