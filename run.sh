# Remove folders of the previous run
hdfs dfs -rm -r ex45_data

hdfs dfs -rm -r ex45_out

# Put input data collection into hdfs
hdfs dfs -put ex45_data

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise45.SparkDriver --deploy-mode cluster --master yarn target/Exercise45-1.0.0.jar "ex45_data/watchedmovies.txt" "ex45_data/preferences.txt" "ex45_data/movies.txt" ex45_out 0.5

