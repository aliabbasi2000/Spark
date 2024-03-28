# Remove folders of the previous run
rm -rf ex45_out

# Run application
spark-submit  --class it.polito.bigdata.spark.exercise45.SparkDriver --deploy-mode client --master local target/Exercise45-1.0.0.jar "ex45_data/watchedmovies.txt" "ex45_data/preferences.txt" "ex45_data/movies.txt" ex45_out 0.5


