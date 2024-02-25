# Remove folders of the previous run
rm -rf ex43_out
rm -rf ex43_out2
rm -rf ex43_out3

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise43.SparkDriver --deploy-mode client --master local target/Exercise43-1.0.0.jar "ex43_data/readings.txt" "ex43_data/neighbors.txt" ex43_out ex43_out2 ex43_out3 3


