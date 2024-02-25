# Remove folders of the previous run
hdfs dfs -rm -r ex43_data

hdfs dfs -rm -r ex43_out
hdfs dfs -rm -r ex43_out2
hdfs dfs -rm -r ex43_out3

# Put input data collection into hdfs
hdfs dfs -put ex43_data

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise43.SparkDriver --deploy-mode cluster --master yarn target/Exercise43-1.0.0.jar "ex43_datas/readings.txt" "ex43_data/neighbors.txt" ex43_out ex43_out2 ex43_out3 3


