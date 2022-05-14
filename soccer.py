from math import inf

import click
from pyspark.sql import SparkSession


@click.command()
@click.option(
    "--soccer-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True),
    help="soccer file",
    required=True,
)
def main(soccer_file: str):
    session = SparkSession.builder.appName("soccer analysis").getOrCreate()
    soccer_df = session.read.csv(soccer_file, sep=",", header=True, inferSchema=True)
    soccer_df.createOrReplaceTempView("soccer")
    result_df = session.sql("SELECT ")


if __name__ == "__main__":
    main()
