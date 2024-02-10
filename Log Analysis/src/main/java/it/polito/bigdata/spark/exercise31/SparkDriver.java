package it.polito.bigdata.spark.exercise31;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String outputPath;
		String inputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #30");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		// Each element/string of the logRDD corresponds to one line of the
		// input file
		JavaRDD<String> logRDD = sc.textFile(inputPath);

		// Solution based on an named class
		// An object of the FilterGoogle is used to filter the content of the
		// RDD.
		// Only the elements of the RDD satisfying the filter imposed by means
		// of the call method of the FilterGoogle class are included in the
		// googleRDD RDD
		JavaRDD<String> googleRDD = logRDD.filter(logLine -> logLine.toLowerCase().contains("google"));

		// Extract the IP address from the selected lines
		// It can be implemented by using the map transformation
		JavaRDD<String> ipRDD = googleRDD.map(logLine -> {
			String[] parts = logLine.split(" ");
			String ip = parts[0];

			return ip;
		});

		// Apply the distinct transformation
		JavaRDD<String> ipDistinctRDD = ipRDD.distinct();

		// Store the result in the output folder
		ipDistinctRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
