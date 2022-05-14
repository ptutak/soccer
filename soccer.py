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
    home_win_rate_df = session.sql(
        """
            SELECT home_team, MAX(home_win_rate) FROM
            (SELECT anon_1.home_team, CAST(win_number as DOUBLE) / CAST(home_matches + away_matches AS DOUBLE) AS home_win_rate FROM
            (select home_team, COUNT(1) AS win_number from soccer WHERE home_score > away_score GROUP BY home_team) as anon_1
            JOIN (SELECT away_team, COUNT(1) as away_matches FROM soccer GROUP BY away_team) as anon_2
            JOIN (SELECT home_team, COUNT(1) AS home_matches FROM soccer GROUP BY home_team) AS anon_3
            on anon_1.home_team = anon_2.away_team AND anon_1.home_team = anon_3.home_team);
        """
    )
    home_win_rate_df.show()

    away_win_rate_df = session.sql(
        """
            SELECT away_team, MAX(away_win_rate) FROM
            (SELECT anon_1.away_team, CAST(win_number as DOUBLE) / CAST(home_matches + away_matches AS DOUBLE) AS away_win_rate FROM
            (select away_team, COUNT(1) AS win_number from soccer WHERE away_score > home_score GROUP BY away_team) as anon_1
            JOIN (SELECT away_team, COUNT(1) as away_matches FROM soccer GROUP BY away_team) as anon_2
            JOIN (SELECT home_team, COUNT(1) AS home_matches FROM soccer GROUP BY home_team) AS anon_3
            on anon_1.away_team = anon_2.away_team AND anon_1.away_team = anon_3.home_team);
        """
    )
    away_win_rate_df.show()


if __name__ == "__main__":
    main()
