# ETL Pipeline Demo

## Summary

This repository holds a demo I put together for a sample Extract-Transform-Load (ETL) pipeline. I'm working with the [GeoNames geographical database](http://www.geonames.org/) from the GeoNames website on this one.

The purpose of this pipeline is to demonstrate the extraction, transformation, and loading of publicly available geography data with PySpark. PySpark is used due to the large volume of data consumed; one of the datasets contains more than 12 million rows. The data itself is of use to geography enthusiasts or professionals who wish to analyze physical geographical features in tandem with the social geographical characteristics of the surrounding state.

### Data Model

The final model includes a fact table, the `geo_info` table, and two dimension tables, the `geonames` table and the `country_info` table. The `geo_info` table was developed to exclude extraneous parameters that likely would not be of immediate interest to a user performing ad-hoc queries. The dimension tables provide additional information that can be further investigated should the user choose to do so.

### Data Dictionary

*geo_info table*

| column | data type | description |
| ------ | --------- | ----------- |
| geoname_id | int | Geographical placename id |
| asciiname | string | ASCII name representation of feature |
| latitude | float | Latitude of feature |
| longitude | float | Longitude of feature |
| feature_class | string | Class of geographical feature |
| country | string | Country in which the feature is found |
| country_code | string | Two-character abbreviation of country |
| neighbours | string | Country codes of the surrounding countries |

*geonames table*

| column | data type | description |
| ------ | --------- | ----------- |
| geoname_id | int | Geographical placename id |
| asciiname | string | ASCII name representation of feature |
| latitude | float | Latitude of feature |
| longitude | float | Longitude of feature |
| feature_class | string | Class of geographical feature |
| feature_code | string | Shortcode of geographical feature |
| country_code | string | Two-character abbreviation of country |
| dem | int | Digital elevation model area in meters |
| timezone | string | Timezone of the feature |

*country_info table*

| column | data type | description |
| ------ | --------- | ----------- |
| ISO | string | ISO code of the country |
| fips | string | FIPS code of the country |
| country | string | Name of the country |
| capital | string | Name of the country's capital |
| area_sq_km | int | Area of the country in sq. km |
| population | int | Population of the country |
| tld | string | Top-level domain of the country |
| currency_name | string | Name of the currency used in the country |
| phone | int | Calling code of the country |
| languages | string | Languages spoken in the country |
| geonameid | int | Geographical placename id of the country |
| neighbours | string | Country codes of the surrounding countries |

## Prerequisites

### Spark

This pipeline uses PySpark to handle the data, so a proper installation is required. You can either use `pip` or download the distribution via the Apache Spark [downloads](https://spark.apache.org/downloads.html) page.

### Data

If you would like to demo the pipeline for yourself you will need to download the appropriate data from the [downloads](https://download.geonames.org/export/dump/) page. The files are large enough that it couldn't be included in the repository as GitHub sets a strict file size limit. The two files I worked with are `countryInfo.txt` and the contents of the `allCountries.zip` file. The only file in `allCountries.zip` is a text file called `allCountries.txt`.

### Poetry

This project utilizes the [Poetry](https://python-poetry.org/docs/#installation) Python package manager to handle dependencies.

## Testing

To test the pipeline for yourself, the items detailed in the [Prerequisites](#prerequisites) section will need to be addressed first. Afterwards, clone the repository to your local machine, step into the root directory, and start the Poetry shell by running

```python
poetry shell
```
You will then be able to install the project dependencies with

```python
poetry install
```

The pipeline is configuration-driven; as such, you will need to make sure that the project paths match that in `config.yaml`. This is important for where you place the downloaded data you will be working with. Place both `countryInfo.txt` and `allCountries.txt` into a folder at the root directory called `data/`, as `config.yaml` looks for the path `data/<file>`.

Finally, to run the pipeline, from the root directory run

```python
python src/etl.py
```
## Future Considerations

### What if the data is increased by 100x?

Though the pipeline is currently run locally, if the volume of data were to become too large, it would be wise to run the pipeline on an AWS EMR cluster. EMR provides enough parallel computing power which bodes well for Spark pipelines.

### What if the pipeline needs to be run daily?

This pipeline is relatively simple, so it can be run daily as a Cron job. If the pipeline were to grow in size or were to start depending on other tasks to be completed prior to running, Apache Airflow would be a great option to schedule multiple tasks in tandem.

### What if the database needs to be accessed by 100+ people?

The data is currently stored locally; for a production-grade solution, the data should be loaded into a cloud database such as AWS Redshift. If the concurrent traffic to the database was consistently high enough that performance issues began to arise, copies of the schema should be made so that individual groups could have their own access points.
