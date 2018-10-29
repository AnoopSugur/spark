import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header","true").option("inferSchema", "true").csv("Netflix_2011_2016.csv")

df.columns
df.printSchema()

for(colName <- Range(0,5)){
  println(df.columns(colName))
}

for(row <- df.head(5)){
  println(row)
}

val df1 = df.withColumn("HV_Ratio", df("High")/df("Volume"))

df1.select("HV_Ratio").show

import spark.implicits._

df.filter("Close > 600").count()
