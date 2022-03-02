from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType
import pyspark.sql.functions as F
from utils import apply_getGroups, apply_top50, apply_top10


if __name__ == "__main__":
    # build spark session
    spark = SparkSession.builder.appName('lastfm').getOrCreate()

    # create schema for the dataframe with the specific data types of each column
    schema = StructType() \
        .add("userid",StringType(),True) \
        .add("timestamp",StringType(),True) \
        .add("musicbrainz-artist-id",StringType(),True) \
        .add("artist-name",StringType(),True) \
        .add("musicbrainz-track-id",StringType(),True) \
        .add("track-name",StringType(),True)
    
    # read the text file with previous schema
    df = spark.read.options(header='False',schema=schema,delimiter='\t') \
        .csv("file:///opt/spark-data/input/lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv")

    # load previous text file into a dataframe with an extra column of time in unix formatt 
    data = df.toDF('userid', \
        'timestamp', \
        'musicbrainz-artist-id', \
        'artist-name', \
        'musicbrainz-track-id', \
        'track-name') \
        .withColumn('eventtime',F.col('timestamp').astype('Timestamp').cast("long"))

    # function to get the session id 
    df_allgroups = apply_getGroups(data)

    # function to get the top 50 sessions based on the number of songs reproduced
    top50_sessions = apply_top50(df_allgroups)

    # write result of top 50 sessions
    top50_sessions.coalesce(1) \
        .write.format('com.databricks.spark.csv') \
        .mode("overwrite") \
        .option("delimiter", "\t") \
        .save('file:///opt/spark-data/output/data_top50_sessions',header = 'true')

    # function to get the top 10 songs more reproduced in the previous top 50 sessions
    top10_songs = apply_top10(df_allgroups,top50_sessions)

    # write result of top 10 songs
    top10_songs.coalesce(1) \
        .write.format('com.databricks.spark.csv') \
        .mode("overwrite") \
        .option("delimiter", "\t") \
        .save('file:///opt/spark-data/output/data_top10_songs',header = 'true')