

mvn clean package


spark-submit --class com.app.workshop.sql.SparkSql --master spark://localhost:7077 target/SparkWorkshop-1.0-SNAPSHOT-jar-with-dependencies.jar


spark-submit --class com.app.workshop.aggregations.SparkAgg --master spark://sambhavs-MacBook-Pro.local:7077 target/SparkWorkshop-1.0-SNAPSHOT-jar-with-dependencies.jar
               --executor-memory 20G \
               --num-executors 50 \
               /path/to/examples.jar \
               
               
               
history server
 /Users/sambhav.gupta/Downloads/spark-2.4.3-bin-hadoop2.7/logs/spark-sambhav.gupta-org.apache.spark.deploy.history.HistoryServer-1-sambhavs-MacBook-Pro.local.out
 
