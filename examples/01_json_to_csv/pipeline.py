"""Runs the Spark Structured Streaming pipeline to convert JSONlines to CSV."""

# pylint: disable=invalid-name,missing-function-docstring

import fire

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery

###############
## ETL Steps ##
###############


def extract(src: str) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    return spark.readStream.option("cleanSource", "delete").json(
        path=src,
        schema="id INT, n DOUBLE",
    )


def transform(df: DataFrame) -> DataFrame:
    return df  # Nothing to do for this example.


def load(df: DataFrame, dst: str) -> StreamingQuery:
    return df.writeStream.trigger(processingTime="15 seconds").start(
        path=dst,
        format="csv",
        outputMode="append",
        checkpointLocation="checkpoints",
    )


##########
## Main ##
##########


def main(src: str, dst: str):
    """Runs the Spark Structured Streaming pipeline.

    Args:
        src: The source directory.
        dst: The destination directory.
    """

    df = extract(src)
    df = transform(df)
    stream = load(df, dst)

    stream.awaitTermination()


if __name__ == "__main__":
    fire.Fire(main)
