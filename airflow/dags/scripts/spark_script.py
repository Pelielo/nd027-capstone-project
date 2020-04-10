from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    IntegerType,
    DateType,
)
import os


conf = SparkConf().setAppName("hello").setMaster("spark://spark-master:7077")


def create_spark_session():
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark


def main():
    spark = create_spark_session()

    input_data_path = "s3a://udacity-dend-14b1/capstone-project/"
    # input_data_path = "data/"
    # output_data_path = "s3a://udacity-dend-14b1/output_data_project4/"
    # output_data_path = "output_data/"

    # get path to song data files
    cities_data_path = "cities/uscities.csv"

    # read song data file
    df = spark.read.csv(input_data_path + cities_data_path, header=True)

    geography_table = (
        df.selectExpr(
            "city_ascii as city",
            "state_name",
            "state_id",
            "'United States' as country",
            "lat as latitude",
            "lng as longitude",
            "density",
        )
        .filter(
            col("state_name").isin(["Puerto Rico", "District of Columbia"]) == False
        )
        .dropDuplicates()
    )
    geography_table.show(5)


if __name__ == "__main__":
    main()
