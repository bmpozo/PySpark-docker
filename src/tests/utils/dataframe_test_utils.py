import unittest

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession



class PySparkTestCase(unittest.TestCase):
    """Set-up of global test SparkSession"""

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[1]")
                     .appName("PySpark unit test")
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


def test_schema(df1: DataFrame, df2: DataFrame, check_nullable=True):
    """Function for comparing two schemas of DataFrames. If schemas are equal returns True.

    Args:
        df1 (DataFrame): transformed DataFrame
        df2 (DataFrame): expected DataFrame
        check_nullable (bool, optional): Additional check to see if the field is nullable based on a flag. Defaults to True.

    Returns:
        _type_: True or False
    """
    field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
    fields1 = [*map(field_list, df1.schema.fields)]
    fields2 = [*map(field_list, df2.schema.fields)]
    if check_nullable:
        res = set(fields1) == set(fields2)
    else:
        res = set([field[:-1] for field in fields1]) == set([field[:-1] for field in fields2])
    return res


def test_data(df1: DataFrame, df2: DataFrame):
    """Function for comparing two DataFrame data. If data is equal returns True.

    Args:
        df1 (DataFrame): transformed DataFrame
        df2 (DataFrame): expected DataFrame

    Returns:
        _type_: True or False
    """
    data1 = df1.collect()
    data2 = df2.collect()
    return set(data1) == set(data2)