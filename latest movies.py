from pyspark.sql.types import *
from pyspark.sql import SparkSession 
pooja=SparkSession.builder.master("local[2]").appName("latest_movies").getOrCreate()
data1=pooja.sparkContext.textFile("C:\\Users\\Pooja\\Desktop\\spark tasks\\Spark_RDD\\Spark_RDD\data\\movies_data\\movies.dat").map(lambda x:x.split("::"))
data2=data1.map(lambda x:(int(x[0]),x[1][0:x[1].rfind("(")],int(x[1][x[1].rfind("(")+1:x[1].rfind(")")]),x[2]))
# print(data2.take(5))



# find latest movies of year 2000....code is incomplete
year=data2.map(lambda x:x[2])
# print(year.max())
year1=year.max()
latestmovies=data2.filter(lambda x:(year1 in x))
# print(latestmovies.count())


# Dataframe schema toDF of data2
df=data2.toDF(["id","movie","year","genere"])
print(df.printSchema())
#print(df.show(10))






# now u can run sql like Querries on DF bt  that,s not SQL

df.createOrReplaceTempView("movies")
#pooja.catalog.listTables()
# pooja.sql("select * from movies").show(50)
# pooja.sql("select * from movies where year=2000").show(10)
# pooja.sql("select distinct(*) from movies").show(10)