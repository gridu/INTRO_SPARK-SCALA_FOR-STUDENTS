# Marketing Analytics

## General requirements
The requirements that apply to project:
* Use gradle to build project (we recommend to use Kotlin DSL, use Getting started to get more information about Gradle)
* Use Spark version 2.4.5 or higher
* The logic should be covered with unit tests (use Scalatest library for unit testing)
* The output should be saved as PARQUET files.
* Configurable input and output for both tasks (use pureconfig library for this goal)
* A spark application has to execute a particular task(=job) based on input arguments
* Use the library scopt to parse the command line arguments
* Content of README files should be:
* Common description of project
* How run unit tests
* How run tasks using spark-submit
* How deploy jobs using Airflow
* Will be a plus: Integrational tests that cover the main method.
  
## Domain
  You work at a data engineering department of a company building an ecommerce platform. There is a mobile application that is used by customers to transact with its on-line store. Marketing department of the company has set up various campaigns (e.g. “Buy one thing and get another one as a gift”, etc.)  via different marketing channels (e.g. Google / Yandex / Facebook Ads, etc.).
  Now the business wants to know the efficiency of the campaigns and channels.


## Data Lake introduction

Firstly, Read the [article](https://medium.com/@lackshub/design-patterns-for-data-lakes-d6da14a0af1f "article")

Every task of the project reflects parts of real Data Lake in the companies. In order to see the big picture of how big companies work with data at scale you’ll see examples of possible transformations around data.

### Given datasets
The generated row dataset is located here: s3://scala-spark-course/dataset/

#### Mobile App clickstream projection (mobile_app_clickstream/ )
Schema:
- userId: String
- eventId: String
- eventTime: Timestamp
- eventType: String
- attributes: Option[Map[String, String]]

There could be events of the following types that form a user engagement session:

- app_open
- search_product
- view_product_details
- purchase
- app_close

Events of app_open type may contain the attributes relevant to the marketing analysis:
- campaign_id
- channel_id

Events of purchase type contain purchase_id attribute.

Notes:
1) It is raw data. It can contain wrong values: eventTime should be greater than 01/01/1974 and eventType shouldn’t be null. You have to exclude these rows from data.
2) User session starts  with app_open and closes with app_close. There aren’t sessions with app_open and without app_close. One user is able to have several sessions. Sessions don't intersect with each other.
3) Recommendation for attribute session_id: should be generated using methods that create some unique value, for instance UUID.

Purchases projection (user_purchases/ )

Schema:
- purchaseId: String
- purchaseTime: Timestamp
- billingCost: Double
- isConfirmed: Boolean

Notes:
1) It is raw data that can contain “dirty” data. In our case it is all rows with billing_cost <= 0

## Tasks
### 1. Clear and prepare raw data

Before processing the data must be prepared
1) Remove all ‘dirty’ data with wrong values
2) Write data as a parquet files. Think about partitioning.

*In real life there are a lot of transformations for raw -> structured zones.*

### 2. Prepare structured data: build Purchases Attribution Projection

   *Companies usually persist this type of data into the structured layer of Data Lake. Reread the part about a structured zone from the [article](https://medium.com/@lackshub/design-patterns-for-data-lakes-d6da14a0af1f "article")*

   The projection is dedicated to enabling a subsequent analysis of marketing campaigns and channels. 

The target schema:
- purchaseId: String
- purchaseTime: Timestamp
- billingCost: Double
- isConfirmed: Boolean

   a session starts with app_open event and finishes with app_close
- sessionId: String 

   In order to create sessionId:
  * Add a column with UUID after aggregation by using an udf function
  * Add a column with UUID in the aggregate function
  
- campaignId: String  // derived from app_open#attributes#campaign_id
- channelIid: String    // derived from app_open#attributes#channel_id

Requirements for implementation of the projection building logic:
#### 2.1 Implement it by utilizing default Spark SQL capabilities.
#### 2.2 Implement it by using a custom [Aggregator](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/expressions/Aggregator.html "Aggregator") or [UDAF](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/expressions/UserDefinedAggregateFunction.html "UDAF").

### 3. Calculate Marketing Campaigns And Channels Statistics

*Data in this layer is ready for reporting and consumption by data analytics.
Reread the part about a trusted zone from the [article](https://medium.com/@lackshub/design-patterns-for-data-lakes-d6da14a0af1f "article")*

Use the purchases-attribution projection to build aggregates that provide the following insights:

#### 3.1 Top Campaigns
- What are the Top 10 marketing campaigns that bring the biggest revenue (based on billingCost of confirmed purchases)?

#### 3.2 Channels engagement performance
- What is the most popular (i.e. Top) channel that drives the highest amount of unique sessions (engagements)  with the App in each campaign?

#### Requirements for task:
- Should be implemented by using plain SQL on top of Spark DataFrame API
- Will be a plus: an additional alternative implementation of the same tasks by using Spark Scala DataFrame / Datasets  API only (without plain SQL)

### 4. Build pipeline using Apache Airflow.

Marketing department decided to see changes every 5 minutes. 
In order to achieve this goal you should create a data pipeline with Airflow.

The instruction you can find in the airflow-pipeline directory.

