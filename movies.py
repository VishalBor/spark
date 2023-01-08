from pyspark.sql.types import *
from pyspark.sql import SparkSession
vishal=SparkSession.builder.master("local[2]").appName("movies").getOrCreate()
data=vishal.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\Spark_RDD\\Spark_RDD\data\\movies_data\\movies.dat").map(lambda x:x.split("::"))
data1=data.flatMap(lambda x:x[2].split("|"))
data2=data1.distinct().sortBy(lambda x:x)
print(data2.take(10))


# task:write the rdd
1111