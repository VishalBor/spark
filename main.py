import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable

from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test1").getOrCreate()

my_list = [1,2,3,4]

df = spark.createDataFrame(my_list,IntegerType())
df.show()
