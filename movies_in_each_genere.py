from pyspark.sql.types import *
from pyspark.sql import SparkSession
vishal=SparkSession.builder.master("local[4]").appName("Generes").getOrCreate()
data=vishal.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\Spark_RDD\\Spark_RDD\data\\movies_data\\movies.dat")
data1=data.map(lambda x:x.split("::")).map(lambda x:x[2]).flatMap(lambda x:x.split("|"))
data2 =data1.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+1)
print(data2.take(9))




