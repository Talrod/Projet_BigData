## Import of libraries and modules

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import mean, min, max
from pyspark.sql.types import IntegerType,DateType,DoubleType
import pyspark.sql.functions as F


## Question 1 - Start a simple Spark Session

spark = SparkSession.builder\
                    .master("local[*]")\
                    .appName("walmart_stock")\
                    .getOrCreate()


## Question 2 - Load the Walmart Stock CSV File

data = spark.read\
          .option("header", True)\
          .csv("walmart_stock.csv")

# Showing top 5 rows
data.show(5)


## Question 3 - What are the column names ?

columns_list = data.columns
print("The columns names are : ", columns_list)


## Question 4 - What does the Schema look like ?

print("The schema looks like this : ")
data.printSchema()

#For the rest of the project, it is necessary to change the type of variables
data = data.withColumn("Date", data["Date"].cast(DateType()))\
           .withColumn("Open", data["Open"].cast(DoubleType()))\
           .withColumn("High", data["High"].cast(DoubleType()))\
           .withColumn("Low", data["Low"].cast(DoubleType()))\
           .withColumn("Close", data["Close"].cast(DoubleType()))\
           .withColumn("Volume", data["Volume"].cast(IntegerType()))\
           .withColumn("Adj Close", data["Adj Close"].cast(DoubleType()))

print("The new schema looks like this : ")
data.printSchema()

#### /!\ Required to make SQL queries
data.createOrReplaceTempView("data_SQL")

## Question 5 - Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day

### Python
CreateHV_ratio = data.withColumn("HV_ratio",col("High")/col("Volume"))
print(CreateHV_ratio.select('HV_ratio').show())

### SQL
spark.sql("""select High/Volume as HV_Ratio from data_SQL""").show()


## Question 6 - What day had the Peak High in Price ?

### Python
print(data.orderBy(col("High")\
          .desc())\
          .select(col("Date"))\
          .head(1))  

### SQL ---
spark.sql("""select Date from data_SQL order by High desc limit 1 """).show()


## Question 7 - What is the mean of the Close column ?

### Python
print("The mean of the Close column is :")
data.select(col("Close"))\
    .agg(avg(col("Close")).alias("Mean"))\
    .show()

### SQL
print("The mean of the Close column is :")
spark.sql("""select mean(Close) as Mean from data_SQL""").show()


## Question 8 - What is the max and min of the Volume column ?

### Python
data.select([min("Volume").alias("Minimum_volume"), max("Volume").alias("Maximum_volume")])\
    .show()

### SQL
spark.sql("""select min(Volume) as `Minimum_volume`, max(Volume) as `Maximum_volume` from data_SQL""").show()


## Question 9 - How many days was the Close lower than 60 dollars ?

### Python
data.filter(col("Close") < 60)\
    .agg(count(col("Date")).alias("DaysNumber"))\
    .show()

### SQL
spark.sql("""select count(Date) as DaysNumber from data_SQL where Close < 60""").show()


## Question 10 - What percentage of the time was the High greater than 80 dollars ?

### Python
perctime = data.filter(col("High")>80).count()/data.count()*100

print('There is {:0.2f}'.format(perctime),"%","of time which High was greater than $80.")

### SQL
spark.sql("""select round(((select count(High) from data_SQL where High > 80)/count(High)*100),2) as Percentage from data_SQL""").show()


## Question 11 - What is the max High per year ?

### Python
data.groupby(year("Date").alias("Year"))\
    .agg(max(col("High")).alias("Max_High"))\
    .orderBy("Year")\
    .show() 

### SQL 
spark.sql("""select year(Date) as Year, max(High) as Max_High from data_SQL group by year order by year""").show()


## Question 12 - What is the average Close for each Calendar Month ?

### Python
data.groupby(year("Date").alias("Year"),month("Date").alias("Month"))\
    .agg(avg(col("Close")).alias("Mean_Close"))\
    .orderBy("Year","Month")\
    .show()

### SQL
spark.sql("""select year(Date) as Year, month(Date) as Month, mean(Close) as Mean from data_SQL group by Year,Month order by Year,Month""").show()
