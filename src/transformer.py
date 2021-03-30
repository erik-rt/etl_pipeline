import os
import pyspark.sql.functions as F

from functools import reduce


def transformer(spark, cfg):
    output = cfg.get('output')
    geonames_cfg = cfg.get('geonames')
    country_info_cfg = cfg.get('country_info')

    geonames_df = spark.read.option('delimiter', '\t').csv(
        geonames_cfg.get('data')
        )
    country_info_df = spark.read.option('delimiter', '\t').csv(
        country_info_cfg.get('data')
        )

    geonames_df = reduce(lambda df, i: df.withColumnRenamed(
        geonames_df.schema.names[i],
        geonames_cfg.get('cols')[i]
        ), range(len(geonames_df.schema.names)), geonames_df)

    country_info_df = reduce(lambda df, i: df.withColumnRenamed(
        country_info_df.schema.names[i],
        country_info_cfg.get('cols')[i]
        ), range(len(country_info_df.schema.names)), country_info_df)

    geonames_table = geonames_df.select(
        F.col('geonameid'),
        F.col('asciiname').alias('name'),
        F.col('latitude'),
        F.col('longitude'),
        F.col('feature_class'),
        F.col('feature_code'),
        F.col('country_code'),
        F.col('dem'),
        F.col('timezone')
    ).drop_duplicates()

    geonames_table.write.mode('overwrite').parquet(
        os.path.join(output, 'geonames/')
    )

    country_info_table = country_info_df.select(
        F.col('ISO'),
        F.col('fips'),
        F.col('country'),
        F.col('capital'),
        F.col('area_sq_km'),
        F.col('population'),
        F.col('tld'),
        F.col('currency_name'),
        F.col('phone'),
        F.col('languages'),
        F.col('geonameid'),
        F.col('neighbours')
    ).drop_duplicates()

    country_info_table.write.mode('overwrite').parquet(
        os.path.join(output, 'country_info/')
    )
