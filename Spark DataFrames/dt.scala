import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")
df.printSchema()

//df.select(year(df("Date"))).show()

val df1 = df.withColumn("Year", year(df("Date")))

val df2 = df1.groupBy("Year").mean()

df2.show()

df2.select("Year", "avg(Close)").show
