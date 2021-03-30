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

    geonames_df = reduce(lambda df, i: df.withColumnRenamed(
        geonames_df.schema.names[i],
        geonames_cfg.get('cols')[i]
    ), range(len(geonames_df.schema.names)), geonames_df)

    geonames_df = geonames_df.select(
        [F.col(col) for col in geonames_cfg.get('final_cols')]
    ).drop_duplicates()

    geonames_df = geonames_df.withColumn(
        'geoname_id', geonames_df.geoname_id.cast('int')
    ).withColumn(
        'latitude', geonames_df.latitude.cast('double')
    ).withColumn(
        'longitude', geonames_df.longitude.cast('double')
    ).withColumn(
        'dem', geonames_df.dem.cast('int')
    )

    geonames_df.write.mode('overwrite').parquet(
        os.path.join(output, 'geonames/')
    )

    country_info_df = spark.read.option('delimiter', '\t').csv(
        country_info_cfg.get('data')
    )

    country_info_df = reduce(lambda df, i: df.withColumnRenamed(
        country_info_df.schema.names[i],
        country_info_cfg.get('cols')[i]
    ), range(len(country_info_df.schema.names)), country_info_df)

    country_info_df = country_info_df.select(
        [F.col(col) for col in country_info_cfg.get('final_cols')]
    ).drop_duplicates()

    country_info_df = country_info_df.withColumn(
        'area_sq_km', country_info_df['area_sq_km'].cast('int')
    ).withColumn(
        'population', country_info_df['population'].cast('int')
    ).withColumn(
        'phone', country_info_df['phone'].cast('int')
    ).withColumn(
        'geonameid', country_info_df['geonameid'].cast('int')
    )

    country_info_df.write.mode('overwrite').parquet(
        os.path.join(output, 'country_info/')
    )

    geo_info_df = geonames_df.join(
        country_info_df,
        (geonames_df.country_code == country_info_df.ISO)
    ).select(
        [F.col(col) for col in cfg.get('geo_info').get('final_cols')]
    ).drop_duplicates()

    geo_info_df = geo_info_df.withColumn(
        'geoname_id', geo_info_df.geoname_id.cast('int')
    ).withColumn(
        'latitude', geo_info_df.latitude.cast('double')
    ).withColumn(
        'longitude', geo_info_df.longitude.cast('double')
    )

    geo_info_df.write.mode('overwrite').parquet(
        os.path.join(output, 'geo_info/')
    )
