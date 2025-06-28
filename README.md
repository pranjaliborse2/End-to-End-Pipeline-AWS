# NFIP Policies Data Pipeline on AWS + Orchestration using Apache Airflow
This project demonstrates an end-to-end data engineering pipeline built on AWS, from raw data ingestion to interactive analytics and visualization. The goal is to efficiently transform and analyze NFIP (National Flood Insurance Program) policy data on the cloud using serverless AWS services.

##  Architecture Overview
Data modelling --> Data Ingestion --> Data Cleaning and Transformation --> Analysis --> Visualization
![image](https://github.com/user-attachments/assets/912c8f7b-6dca-439d-9dcf-20eae0ba0fbe)
+ **Postgres and Python:** Creates a relational database from a consolidated Policies dataset, queries the data for regions of interest and stores them into different CSV files for ease of analysis.
+ **S3:** Stores raw CSV data and processed Parquet files for three states CA, NY and TX in partitioned folder structure adapted for query optimization in further analysis.
+ **AWS Glue:** Crawls S3 to catalog schema in the Data Catalog. Performs schema transformation and data cleaning using PySpark.
+ **AWS Lambda:** Triggers the Glue crawler upon new Parquet file uploads after ETL job and crawls them to Glue data catalog.
+ **AWS Athena:** Performs cost-optimized querying using SQL.
+ **Amazon QuickSight:** Visualizes insights directly from Athena queries.
+ **DAG Orchestration:** Automates the workflow using Airflow DAG by using AWS service operators locally via python
  
## Pipeline Workflow
+ **Data Upload:**  Raw .dump file is converted to a readable format and uploaded to S3.
+ **ETL job:**  Glue ETL cleans and converts data to optimized Parquet format.
+ **Automatic Trigger:**  Lambda detects a new upload. Starts Glue Crawler to catalog the schema.
+ **Partitioning and Optimization:**  Data is partitioned by state (propertyState) for efficient Athena queries.
+ **Analysis in Athena:**  Query policies by region, year, coverage, and premiums. Identify trends and outliers across states.
+ **Visualization in QuickSight:**  Dashboard built to show trends in policy counts, premiums, and risk areas

Developed an automated Workflow of Steps from initial Glue crawling to Analysis in Athena using Apache-Airflow DAG.
### Sample Athena query
SELECT   
  propertyState,  
  COUNT(*) AS policy_count,  
  ROUND(AVG(totalInsurancePremiumOfThePolicy), 2) AS avg_premium  
FROM "nfip-policies-db"."policies_by_state"  
WHERE policyeffectivedate BETWEEN DATE '2019-01-01' AND DATE '2023-12-31'  
GROUP BY propertyState  
ORDER BY avg_premium DESC

 ## Features
+ 🗃️ Schema inference via Glue Crawlers
+ 🧼 Data type resolution with DynamicFrame transformations
+ 💰 Cost-efficient querying in Athena
+ 📈 Clean dashboard built using QuickSight
+ ⚙️ Fully serverless and scalable

##  Key Insights
+ High level analysis suggested that while Texas shows the highest number of flood insurance policies, average premium of policy is highest in New York and lowest in Texas. However, average coverage tends to be similar for all three states.
+ Many records required schema harmonization due to inconsistent formats in reportedZipCode and policyEffectiveDate.
