from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

# ── 1. Create a Spark session ──────────────────────────────────────────────
# We connect to MongoDB using the mongo-spark-connector package.
# The URI points to the MongoDB container by its Docker network hostname.
spark = SparkSession.builder \
    .appName("FootballLeagueStandings") \
    .config("spark.mongodb.write.connection.uri",
            "mongodb://football_mongodb:27017/football_db.standings") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── 2. Load raw match data ─────────────────────────────────────────────────
# Spark reads the JSON file you created in Step 3.
# Each row represents one match: home_team, away_team, home_goals, away_goals.
matches_df = spark.read.json("/opt/spark/data/match_results.json")
matches_df.printSchema()
matches_df.show()

# ── 3. Calculate stats for the HOME team in each match ─────────────────────
home_df = matches_df.select(
    col("home_team").alias("team"),
    col("home_goals").alias("scored"),
    col("away_goals").alias("conceded"),
    when(col("home_goals") > col("away_goals"), 1).otherwise(0).alias("wins"),
    when(col("home_goals") == col("away_goals"), 1).otherwise(0).alias("draws"),
    when(col("home_goals") < col("away_goals"), 1).otherwise(0).alias("losses"),
)

# ── 4. Calculate stats for the AWAY team in each match ─────────────────────
away_df = matches_df.select(
    col("away_team").alias("team"),
    col("away_goals").alias("scored"),
    col("home_goals").alias("conceded"),
    when(col("away_goals") > col("home_goals"), 1).otherwise(0).alias("wins"),
    when(col("away_goals") == col("home_goals"), 1).otherwise(0).alias("draws"),
    when(col("away_goals") < col("home_goals"), 1).otherwise(0).alias("losses"),
)

# ── 5. Union home + away rows, then aggregate by team ──────────────────────
# This gives us one row per team with totals across all their matches.
all_df = home_df.union(away_df)
all_df = all_df.filter(col("team").isNotNull())

standings_df = all_df.groupBy("team").agg(
    F.count("*").alias("played"),
    F.sum("wins").alias("wins"),
    F.sum("draws").alias("draws"),
    F.sum("losses").alias("losses"),
    F.sum("scored").alias("goals_for"),
    F.sum("conceded").alias("goals_against"),
)

# ── 6. Add calculated columns ──────────────────────────────────────────────
# Points: 3 for a win, 1 for a draw, 0 for a loss
# Goal difference: goals scored minus goals conceded
standings_df = standings_df \
    .withColumn("goal_difference",
                col("goals_for") - col("goals_against")) \
    .withColumn("points",
                (col("wins") * 3) + (col("draws") * 1))

# ── 7. Sort by points descending (then goal difference as tiebreaker) ───────
standings_df = standings_df.orderBy(
    col("points").desc(),
    col("goal_difference").desc()
)

standings_df.show(truncate=False)

# ── 8. Write the final standings to MongoDB ────────────────────────────────
# 'overwrite' mode replaces the collection on every run — good for a pipeline.
standings_df.write \
    .format("mongodb") \
    .mode("overwrite") \
    .save()

print("✅ Standings written to MongoDB successfully.")
spark.stop()