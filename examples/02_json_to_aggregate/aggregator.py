"""Runs the Spark Structured Streaming to aggregate the generated file outputs."""

# pylint: disable=invalid-name,missing-function-docstring

import fire

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.streaming import StreamingQuery

###############
## ETL Steps ##
###############


def extract(fpath: str) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    return spark.readStream.json(path=fpath, schema="id INT, n DOUBLE")


def transform(df: DataFrame) -> DataFrame:
    return df.agg(
        F.count("*").alias("count"),
        F.sum("n").alias("sum"),
        F.avg("n").alias("avg"),
    )


def load(df: DataFrame) -> StreamingQuery:
    return df.writeStream.start(format="console", outputMode="complete")


##########
## Main ##
##########


def main(src: str):
    """Runs the Spark Structured Streaming pipeline."""

    df = extract(src)
    df = transform(df)
    stream = load(df)

    stream.awaitTermination()


if __name__ == "__main__":
    fire.Fire(main)
