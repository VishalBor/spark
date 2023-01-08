from pyspark.sql.types import *
from pyspark.sql import SparkSession
vishal=SparkSession.builder.master("local[5]").appName("Generes").getOrCreate()
data=vishal.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\Spark_RDD\\Spark_RDD\data\\movies_data\\movies.dat")
data1=data.map(lambda x:x.split("::")).map(lambda x:x[1]).map(lambda x:x[0:1]).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x)
isalpha=data1.filter(lambda x:x[0].isalpha())
isdigit=data1.filter(lambda x:x[0].isdigit())
print(isalpha.take(20))
print(isdigit.take(20))