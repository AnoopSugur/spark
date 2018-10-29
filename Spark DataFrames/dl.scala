import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header","true").option("inferSchema", "true").csv("CitiGroup2006_2008")

//df.head(5)


for(n <- df.head(6)) {
  println(n)
}

df.columns

df.describe().show()

df.select("Volume", "Date").show()

val df1 = df.withColumn("HighPlusLow",df("High")+df("Low"))

val df2 = df1.select("HighPlusLow").as("HPL")

df2.show()
