import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
val df = spark.read.option("header","true").option("inferSchema","true").csv("Sales.csv")

// Show Schema
//df.printSchema()

//df.show()

df.groupBy("Company")

df.groupBy("Company").mean("Sales").show()
df.groupBy("Company").mean().show() // Spark automatically picks all the Numerical columns

df.groupBy("Company").count().show()
df.groupBy("Company").max().show()

df.select(countDistinct("Sales")).show() //approxCountDistinct
df.select(sumDistinct("Sales")).show()
df.select(variance("Sales")).show()
df.select(stddev("Sales")).show() //avg,max,min,sum,stddev
df.select(collect_set("Sales")).show()


df.orderBy("Sales", "desc")show()

df.orderBy($"Sales".desc).show()


val df1 = df.select().groupBy("Company").mean("Sales").as("avg_sales")
df1.orderBy("Company").show()
