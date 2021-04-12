# BigData Tasks: Marketing Analytics

### Domain

You work at a data engineering department of a company building an ecommerce platform. There is a mobile application 
that is used by customers to transact with its on-line store. Marketing department of the company has set up various 
campaigns (e.g. “Buy one thing and get another one as a gift”, etc.)  via different marketing channels 
(e.g. Google / Yandex / Facebook Ads, etc.).

Now the business wants to know the efficiency of the campaigns and channels.

Let’s help them out!

### Given datasets

Generated dataset: https://github.com/gridu/INTRO_SPARK-SCALA_FOR_STUDENTS

### Mobile App clickstream projection

Schema:
* userId: String
* eventId: String
* eventTime: Timestamp
* eventType: String
* attributes: Option[Map[String, String]]
  
There could be events of the following types that form a user engagement session:

* app_open
* search_product
* view_product_details
* purchase
* app_close

Events of `app_open` type may contain the attributes relevant to the marketing analysis:

* campaign_id
* channel_id

Events of `purchase` type contain `purchase_id` attribute.
    
### Purchases projection

Schema:
* purchaseId: String
* purchaseTime: Timestamp
* billingCost: Double
* isConfirmed: Boolean

## Tasks & Requirements

### Task #1. Build Purchases Attribution Projection

The projection is dedicated to enabling a subsequent analysis of marketing campaigns and channels.

The target schema:
* purchaseId: String
* purchaseTime: Timestamp
* billingCost: Double
* isConfirmed: Boolean

// a session starts with `app_open` event and finishes with `app_close`

* sessionId: String
* campaignId: String  // derived from `app_open#attributes#campaign_id`
* channelId: String    // derived from `app_open#attributes#channel_id`

Requirements for implementation of the projection building logic:
1. Implement it by utilizing default Spark SQL capabilities.
2. Implement it by using a custom [Aggregator](https://www.google.com/url?q=https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/expressions/Aggregator.html&sa=D&source=editors&ust=1617778292448000&usg=AOvVaw0d6lVgo7BM5Iii7R9gBFSA)
or [UDAF](https://www.google.com/url?q=https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/expressions/UserDefinedAggregateFunction.html&sa=D&source=editors&ust=1617778292448000&usg=AOvVaw2bOhURBkOeQtVq1b6Q8Pab).

### Task #2. Calculate Marketing Campaigns And Channels Statistics

Use the purchases-attribution projection to build aggregates that provide the following insights:
1. Top Campaigns: - What are the Top 10 marketing campaigns that bring the biggest `revenue`
   (based on `billingCost` of confirmed purchases)?
2. Channels engagement performance: - What is the `most popular (i.e. Top) channel` that drives the highest amount of 
   unique sessions (engagements)  with the App in `each campaign`?

Requirements for task #2:
* Should be implemented by using plain SQL on top of Spark DataFrame API
* Will be a plus: an additional alternative implementation of the same tasks by using Spark Scala DataFrame / Datasets  
  API only (without plain SQL)

### Task #3. (optional) Organize data warehouse and calculate metrics for time period.

1. Convert input dataset to parquet. Think about partitioning. Compare performance on top CSV input and parquet input. 
   Save output for Task #1 as parquet as well.
2. Calculate metrics from Task #2 for different time periods:
    * For September 2020
    * For 2020-11-11
      
Compare performance on top csv input and partitioned parquet input. Print and analyze query plans (logical and physical) 
for both inputs.

Requirements for Task 3:
* General input dataset should be partitioned by date
* Save query plans as *.MD file. It will be discussed on exam.

Build  Weekly purchases Projection within one quarter

## General requirements

The requirements that apply to all tasks:
* Use Spark version 2.4.5 or higher
* The logic should be covered with unit tests
* The output should be saved as PARQUET files.
* Configurable input and output for both tasks
* Will be a plus: README file in the project documenting the solution.
* Will be a plus: Integrational tests that cover the main method.
