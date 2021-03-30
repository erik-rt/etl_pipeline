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
        [F.col(col) for col in geonames_cfg.get('final_cols')]
    ).drop_duplicates()

    geonames_table.write.mode('overwrite').parquet(
        os.path.join(output, 'geonames/')
    )

    country_info_table = country_info_df.select(
        [F.col(col) for col in country_info_cfg.get('final_cols')]
    ).drop_duplicates()

    country_info_table.write.mode('overwrite').parquet(
        os.path.join(output, 'country_info/')
    )

    geo_info_table = geonames_df.join(
        country_info_df,
        (geonames_df.country_code == country_info_df.ISO)
    ).select(
        [F.col(col) for col in cfg.get('geo_info').get('final_cols')]
    ).drop_duplicates()

    geo_info_table.write.mode('overwrite').parquet(
        os.path.join(output, 'geo_info/')
    )
