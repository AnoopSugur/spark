import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()

val df = spark.read.option("header", "true").option("inferSchema", "true").csv("CitiGroup2006_2008")

df.printSchema()

import spark.implicits._

//scala notation
df.filter($"Close">480).show()
df.filter($"Close" < 480 && $"High" <480).show

//sql notation
df.filter("Close > 480").show()
df.filter("Close < 480 AND High <480").show()

val low = df.filter("Close < 480 AND High <480").collect()

println(low)

df.filter("High == 484.40").show()

df.select(corr("High", "Low")).show()
