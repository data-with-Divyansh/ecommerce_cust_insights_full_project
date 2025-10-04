import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *

os.environ['PYSPARK_PYTHON'] = r'C:\Users\PCNAME\AppData\Local\Programs\Python\Python310\python.exe'
# os.environ['HADOOP_HOME'] = r'C:\hadoop'
# os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk1.8.0_202'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

print("================= PYSPARK Started =====================")
print()

orders_df = spark.read.csv("orders_large.csv", header="true").orderBy("customer_id").withColumn("currency",lit("USD"))
orders_df.show()

refund_df = spark.read.csv("refunds_large.csv", header="true").withColumnRenamed("customer_id","cust_id")
refund_df.show()


joindf = (orders_df.join(refund_df,"order_id","left")
          .withColumn("refund_amount",col("refund_amount").cast("double"))
          .fillna(0,subset=["refund_amount"])
          )
joindf.show(50)
joindf.printSchema()

total_df = joindf.groupBy("customer_id").agg(sum("amount").alias("spend"))
total_df.show()

return_total_df = joindf.groupBy("customer_id").agg(sum("refund_amount").alias("refund_total"))
return_total_df.show()

joindf2 = (joindf.join(total_df,"customer_id","left")
           .join(return_total_df,"customer_id","left")
           .withColumn("total_spend",expr("spend - refund_total"))
           )
joindf2.show()

tier_final_df = (joindf2.withColumn("tier",expr("case when total_spend > 2000 then 'Gold' when total_spend > 1000 then 'Silver' else 'Bronze' end"))
                 .drop("currency","order_status","subscription_status","cust_id")
                 .orderBy("customer_id",ascending=True)
                 )
tier_final_df.show()

tier_final_df.coalesce(1).write.mode("overwrite").partitionBy("tier").csv("Output/tier_data",header=True)



