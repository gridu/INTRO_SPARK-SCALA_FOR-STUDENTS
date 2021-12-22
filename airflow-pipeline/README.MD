## Init project

1.1 Install gradle
    
    https://docs.gradle.org/current/userguide/installation.html
    
1.2 Init gradle project


    gradle init

    
1.3 Use build.gradle from airflow-pipeline directory


## Setup airflow project

### Prepage amazon S3

Create your own directory into the common bucket “student-labs” on S3 to persist job artifacts.
URI to access the directory can look like:

```s3a://student-labs/ivanov```

### Codebase preparation

1. Change patches in the file configuration according to your S3 directory. Notice that path must be started with s3a:// to use S3A committer.

   Add patches to datasets:

   ```s3a://scala-spark-course/dataset/mobile_app_clickstream/mobile_app_clickstream_0.csv.gz```
   
   ```s3a://scala-spark-course/dataset/user_purchases/user_purchases_0.csv.gz``` 


2. Remove from the SparkSession configuration the master’s settings.

3. Compile the fat jar with dependencies and the configuration file by using command

   ```gradle shadowJar```

4. Upload the compiled jar into your S3 directory.

### Prepare Airflow

1. Create the directory “dags” and copy the file spark_dag.py inside
2. Run the docker compose (docker-compose up)
3. Open the airflow admin page by url: http://localhost:8080/home/

Credentials: user: airflow; password: airflow

4. Add connection settings for Amazon EMR.

      Read the article for more details: https://vulpala.com/2019/08/22/spin-aws-emr-cluster-using-apache-airflow/

5. Open the file spark_dag and change the value SPARK_STEPS according to the pipeline:
   * Converting csv to parquet;
   * Session projection;
   * Top 10 campaigns;
   * Top channels.

6. Add your jar path on S3, program arguments and the main class name to perform the pipeline above.

7. Open the airflow admin page at “DAGs” tab and find DAG with the name marketing_data_lake. Open the Dag and run.

   Check airflow UI documentation for details:  
   https://airflow.apache.org/docs/apache-airflow/stable/ui.html

9. Open Amazon EMR service on the Amazon website and find your cluster in proper state.
10. Check S3 directory on proper result files when the pipeline finished.

Airflow documentation:
https://airflow.apache.org/docs/apache-airflow/stable/ui.html


Amazon EMR documentation:
https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-overview.html


