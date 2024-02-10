# Remove folders of the previous run
hdfs dfs -rm -r ex31_data
hdfs dfs -rm -r ex31_out

# Put input data collection into hdfs
hdfs dfs -put ex31_data

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise31.SparkDriver --deploy-mode cluster --master yarn target/Exercise31-1.0.0.jar "ex31_data/log.txt" ex31_out/


