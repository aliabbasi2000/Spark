# Remove folders of the previous run
hdfs dfs -rm -r ex44_data

hdfs dfs -rm -r ex44_out

# Put input data collection into hdfs
hdfs dfs -put ex44_data

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise44.SparkDriver --deploy-mode cluster --master yarn target/Exercise44-1.0.0.jar "ex44_data/watchedmovies.txt" "ex44_data/preferences.txt" "ex44_data/movies.txt" ex44_out 0.5

