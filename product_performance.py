import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
from pyspark import StorageLevel
import time


os.environ['PYSPARK_PYTHON'] = r'C:\Users\Quantumn\AppData\Local\Programs\Python\Python310\python.exe'
# os.environ['HADOOP_HOME'] = r'C:\hadoop'
# os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk1.8.0_202'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

start = time.time()

print("================= PYSPARK Started =====================")
print()

########################### Read the customer data ###########################

cust_df = spark.read.csv("orders_large.csv", header=True).orderBy("customer_id", ascending=False)
cust_df.show()


distinct_orders = cust_df.select("order_status").distinct() # To find all the distinct order_status for further use
distinct_orders.show()

######################## Finding the number of completed orders ############################

completed_order_df = (cust_df.filter(col("order_status")=='completed')
                      .groupBy("product_id").agg(count("product_id").alias("completed_orders"))
                      )
completed_order_df.show()

######################## Finding the number of pending and refunded orders ############################

refunded_order_df = (cust_df.filter((col("order_status").isin(['pending','refunded'])))
                     .groupBy("product_id").agg(count("product_id").alias("refund_or_pending_orders"))
                     )
refunded_order_df.show()

######################### Finding the number of active subscriptions for the particular products #######################
active_df = (cust_df.filter(col("subscription_status")=='active')
                       .groupBy("product_id").agg(count("product_id").alias("active_subs"))
                       )
active_df.show()
######################## Finding the number of cancelled subscriptions ############################
cancelled_df = (
    cust_df.filter(col("subscription_status")=='canceled')
    .groupBy("product_id").agg(count("product_id").alias("cancelled_subs"))
)
cancelled_df.show()

######################## Join the both active and cancelled subscriptions dataframes and add them to find total subscriptions ##########################
subs_join_df = (active_df.join(cancelled_df,"product_id","full")
                .withColumn("total_subs",expr("active_subs + cancelled_subs"))
                )
subs_join_df.show()

######################## Join the both active and cancelled subscriptions dataframes and add them to find total subscriptions ##########################
sales_join_df = (completed_order_df.join(refunded_order_df,"product_id","full")
                 .withColumn("total_sales",expr("completed_orders + refund_or_pending_orders"))
                 )
sales_join_df.show()

orders_join_df = sales_join_df.join(subs_join_df,"product_id","full")
orders_join_df.show()


#################### Read Click Stream Data {JSON} file #################

clickstream_df = spark.read.json("clickstream.json", multiLine=True).orderBy("product_id",ascending=True)
clickstream_df.show()

events_df =clickstream_df.select("event_type").distinct() # To find all the distinct event_type for further use
events_df.show()

################### Find total number for page views for every product #####################

page_view_df = (clickstream_df.filter(col("event_type")=='page_view')
                .groupBy("product_id").agg(count("product_id").alias("page_views"))
                .orderBy("page_views", ascending=False)
                )
page_view_df.show()
clickstream_df.show(100)

################### Finding no of events by every user for every product ####################

event_count_df = (clickstream_df.groupBy("product_id","user_id").agg(count("user_id").alias("event_count"))
                  .orderBy("product_id",ascending=True))
event_count_df.show(50)

################### No of events will help us find abandoned carts. Event Funnel = 1 (Page view) > 2 (Add to cart) > 3 (Checkout) > 4 (Purchase)
################### If a user has 4 events recorded for the same product then he has purchased the product.
################### If a user has 2 or 3 events recorded for the same product it simply means that they have not made the purchase and that they have either stopped after add to cart or checkout stage

abandoned_carts_df = (event_count_df.filter(col("event_count").isin([2,3]))
                .groupBy("product_id").agg(count("product_id").alias("abandoned_cart"))
                .orderBy("product_id", ascending=True)
                )
abandoned_carts_df.show()

clickstream_join_df = page_view_df.join(abandoned_carts_df,"product_id","full")
# clickstream_join_df.show()

final_join_df = orders_join_df.join(clickstream_join_df,"product_id","full")
final_join_df.show(50)

final_join_df.coalesce(1).write.csv("Output/product_performance", header=True)

end = time.time()
print(f"Runtime: {end - start:.2f} seconds")
