# Remove folders of the previous run
rm -rf ex44_out

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise44.SparkDriver --deploy-mode client --master local target/Exercise44-1.0.0.jar "ex44_data/watchedmovies.txt" "ex44_data/preferences.txt" "ex44_data/movies.txt" ex44_out 0.5


