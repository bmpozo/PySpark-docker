from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def apply_getGroups(df_input: DataFrame) -> DataFrame:
    """Function to generate the session id based on if the previous
    song of current row keeps in under 20 minutes of difference. Columns details
    - songs_20_min : count of number of songs in a range of 20 minutes before of timestamp of current row
    - is_new_session : binary column. If song_20_min is 1 that means a new session has started,
    it is different of 1 that means the sessions continues
    - unique_id : to have a column with descending unique identifier with that keeps the order in the column is_new_session
    - group_session_id : acumulative sum based on the binary column is_new_session. If there is no new session value is 0,
    for new sessions value is 1. With acumulative sum aggregation at column level keeping the order of the rows
    it generates ids for sessions

    Args:
        df_input (DataFrame): dataframe with the initial value of lastfm sample

    Returns:
        DataFrame: dataframe with the session id generated
    """
    w_1 = Window.partitionBy(F.col('userid')).orderBy(F.col('eventtime').desc()).rangeBetween(-1201,Window.currentRow)
    w_2 = Window.orderBy('unique_id')

    data_sessions = df_input.select(F.col('userid'),F.col('musicbrainz-track-id'),F.col('track-name'),F.col('timestamp'),F.col('eventtime')) \
        .withColumn('songs_20_min',F.count('musicbrainz-track-id').over(w_1)) \
        .withColumn('is_new_session',F.when(F.col('songs_20_min')==1,1).otherwise(0)) \
        .withColumn('unique_id',F.monotonically_increasing_id()) \
        .withColumn('group_session_id',F.sum('is_new_session').over(w_2))
    
    return data_sessions

def apply_top50(df_all: DataFrame) -> DataFrame:
    """Function to return the top 50 sessions with the highest number of songs.
    Group by session id and count the number of songs in each one.
    Generate row number to have an id that is descending based on the number for songs.
    Filter by the first 50 rows

    Args:
        df_all (DataFrame): dataframe of lastfm sample with session id column

    Returns:
        DataFrame: dataframe with top 50 sessions with the number of songs reproduced
    """
    w_3 = Window.orderBy(F.col('number_songs').desc())

    data_top50_sessions = df_all \
    .groupBy(F.col('userid'),F.col('group_session_id')) \
    .agg(F.count(F.col('track-name')) \
    .alias('number_songs')) \
    .withColumn('order_top_sessions',F.rank().over(w_3)) \
    .filter(F.col('order_top_sessions') <= 50)

    return data_top50_sessions

def  apply_top10(df_all: DataFrame, df_top50: DataFrame) -> DataFrame:
    """Function to return top 10 songs that belong to the top 50 sessions.
    To get the songs, original lastfm dataframe with session id is filtered
    using the top 50 to get the ids for users and session.
    Group by songs and count number of rows in unique_ids column.
    Generate row number to have an id that is descending based on the number fo repitions.
    Filter by the first 10 rows

    Args:
        df_all (DataFrame): dataframe of lastfm sample with session id column
        df_top50 (DataFrame): dataframe with top 50 sessions with the number of songs reproduced

    Returns:
        DataFrame: dataframe with top 10 songs with the number of songs reproduced
    """
    w_4 = Window.orderBy(F.col('number_reps').desc())

    data_top10_song = df_all.join(df_top50, ['userid','group_session_id'])

    data_top10_song \
    .groupBy(F.col('musicbrainz-track-id'),F.col('track-name')) \
    .agg(F.count(F.col('unique_id')).alias('number_reps')) \
    .withColumn('order_top_songs',F.rank().over(w_4)) \
    .filter(F.col('order_top_songs') <= 10) \
    .select(F.col('track-name'),F.col('number_reps'),F.col('order_top_songs'))

    return data_top10_song