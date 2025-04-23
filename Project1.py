from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType


spark = SparkSession.builder.appName("SparkSQLProject").getOrCreate()
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("movie_id", StringType(), True),
    StructField("timestamp", DateType(), True),
    StructField("duration_watched", IntegerType(), True)
])

# Load Data as RDD
df = spark.read.schema(schema).csv(
    "file:///C:/SparkCourse/Spark_Projects/movie_watch_analysis/watch_logs.csv")

# Filter out records with duration < 200 seconds
moviewtchtime = df.filter(df.duration_watched < 200)

# Register as temp table
moviewtchtime.createOrReplaceTempView("temptable")

# Top watched movies
topwatch = spark.sql(
    "select movie_id, COUNT(*) AS short_watch_count from temptable group By movie_id order by short_watch_count DESC LIMIT 5")
topwatch.show()

# Users with binge-watching patterns (watching >3 movies/day)
bingewatch = spark.sql(
    "SELECT user_id, COUNT(*) as movies_watched FROM temptable GROUP BY user_id HAVING movies_watched > 3 ORDER BY movies_watched DESC LIMIT 10")
bingewatch.show()

# Add a UDF: Classify Viewer Types


def viewtype(duration):
    if duration is None:
        return "Unknown"
    elif duration < 1800:  # less than 30 minutes
        return "Light Viewer"
    elif duration < 5400:  # less than 90 minutes
        return "Moderate Viewer"
    else:
        return "Heavy Viewer"


# # Register UDF
viewer_type_udf = udf(viewtype, StringType())

# # Apply UDF
watchlogs_df = df.withColumn(
    "viewer_type_Column", viewer_type_udf("duration_watched"))

watchlogs_df.show()

watchlogs_df.write.mode("overwrite").option("header", True).csv(
    "file:///C:/SparkCourse/Spark_Projects/movie_watch_analysis/output/watchlogs_classified.csv")
