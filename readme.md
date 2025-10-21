## 🛒 E-Commerce Customer Insights<br>
### 📊 Project Overview

ecommerce_customer_insights is a data analytics project that uncovers customer behavior patterns from e-commerce transactions.
The goal is to: <br>
Segment customers into tiers — Gold, Silver, and Bronze — based on their total spending.<br>
Analyze churn for subscription-based customers.<br>
Provide actionable insights to improve retention and marketing strategies.<br>

This project demonstrates skills in data engineering, analytics, and visualization using modern tools such as Python, Spark, and Kafka (optional if you used them).

## Project Name: Customer, product subscription and product insights

### Task 1 (Python script: cust_tier.py) - Task information: This project is about performing the following tasks:
1. Tiering customers has spent a particular amount:<br>
    A. If a customer has spent more than 2000 then mark it as Gold tier<br>
    B. If a customer has spent more than 1000 then mark it as Silver tier<br>
    C. If less than 1000 then mark as Bronze.<br>
2. Find out all the necessary product information and join it into a single table/dataframe.


### Task 2 (Python script: product_performance.py) - Task information: 
Source for data: There are 3 source files in the project:
1. orders_large.csv
2. refunds_large.csv
3. clickstream.json

- Using the source data find product information: page_views, abandoned carts, active and cancelled subscription.
- Combine all the product data into 1 dataframe for complete product insights.

### Output Information:
Output files will be written to the Output folder in the project with a single file.
Task 1 data is written using Partitioning the tier column, hence it will write Gold, Silver and Bronze records in three different folders.

#### Some data values may seem irrational since this is just a sample project for understanding transformations of raw data.
