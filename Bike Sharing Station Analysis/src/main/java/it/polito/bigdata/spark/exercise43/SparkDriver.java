package it.polito.bigdata.spark.exercise43;

import org.apache.spark.api.java.*;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPathReadings;
		String inputPathNeighbors;
		String outputPath;
		String outputPath2;
		String outputPath3;
		int threshold;

		inputPathReadings = args[0];
		inputPathNeighbors = args[1];
		outputPath = args[2];
		outputPath2 = args[3];
		outputPath3 = args[4];
		threshold = Integer.parseInt(args[5]);

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #43");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *******************************************************************
		// *******************************************************************
		// Part 1
		// Selection of the stations with a percentage of critical situations
		// greater than 80%

		// Read the content of the readings file
		JavaRDD<String> readingsRDD = sc.textFile(inputPathReadings).cache();

		// Count the number of total and critical readings for each station

		// Create a PairRDD with stationID as key and a Count object as value
		// Count has two values:
		// - numReadings: 1 for one single line
		// - numCriticalReadings: 0 if the situation is not critical. 1 if it is
		// critical
		JavaPairRDD<String, Count> stationCountPairRDD = readingsRDD.mapToPair(line -> {
			String[] fields = line.split(",");

			// fields[0] is the station id
			// fields[5] is the number of free slots
			if (Integer.parseInt(fields[5]) < threshold)
				return new Tuple2<String, Count>(fields[0], new Count(1, 1));
			else
				return new Tuple2<String, Count>(fields[0], new Count(1, 0));
		});

		// Compute the number of total and critical readings for each station
		JavaPairRDD<String, Count> stationTotalCountPairRDD = stationCountPairRDD.reduceByKey(new Sum());

		// Compute the percentage of critical situations for each station
		JavaPairRDD<String, Double> stationPercentagePairRDD = stationTotalCountPairRDD.mapValues(new Percentage());

		// Select stations with percentage > 80%
		JavaPairRDD<String, Double> selectedStationsPairRDD = stationPercentagePairRDD
				.filter(new CriticalPercentage(0.8));

		// Change key,value roles
		JavaPairRDD<Double, String> percentageStationsPairRDD = selectedStationsPairRDD.mapToPair(new InvertPair());

		percentageStationsPairRDD.sortByKey(false).saveAsTextFile(outputPath);

		// *******************************************************************
		// *******************************************************************
		// Part 2
		// Selection of the pairs (timeslot, station) with a percentage of
		// critical situations
		// greater than 80%

		// The input data are already in readingsRDD

		// Count the number of total and critical readings for each pair
		// (timeslot,station)

		// Create a PairRDD with timeslot_stationID as key and a Count object as
		// value
		// Count has two values:
		// - numReadings: 1 for one single line
		// - numCriticalReadings: 0 if the situation is not critical. 1 if it is
		// critical
		JavaPairRDD<String, Count> timestampStationCountPairRDD = readingsRDD.mapToPair(line -> {
			String[] fields = line.split(",");

			// fields[0] is the station id
			// fields[2] is the hour
			// fields[5] is the number of free slots

			int minTimeslotHour = 4 * (Integer.parseInt(fields[2]) / 4);
			int maxTimeslotHour = minTimeslotHour + 3;

			String timestamp = new String("ts[" + minTimeslotHour + "-" + maxTimeslotHour + "]");

			// Concatenate time slot and stationid
			// A specific class could be defined and used instead of the
			// concatenation of the strings
			String key = new String(timestamp + "_" + fields[0]);

			if (Integer.parseInt(fields[5]) < threshold)
				return new Tuple2<String, Count>(key, new Count(1, 1));
			else
				return new Tuple2<String, Count>(key, new Count(1, 0));
		});

		// Compute the number of total and critical readings for each pair
		// (timeslot,station)
		JavaPairRDD<String, Count> timestampStationTotalCountPairRDD = timestampStationCountPairRDD
				.reduceByKey(new Sum());

		// Compute the percentage of critical situations for each pair
		// (timeslot,station)
		JavaPairRDD<String, Double> timestampStationPercentagePairRDD = timestampStationTotalCountPairRDD
				.mapValues(new Percentage());

		// Select stations with percentage > 80%
		JavaPairRDD<String, Double> selectedTimestampStationsPairRDD = timestampStationPercentagePairRDD
				.filter(new CriticalPercentage(0.8));

		// Change key,value roles
		JavaPairRDD<Double, String> percentageTimestampStationsPairRDD = selectedTimestampStationsPairRDD
				.mapToPair(new InvertPair());

		percentageTimestampStationsPairRDD.sortByKey(false).saveAsTextFile(outputPath2);

		// *******************************************************************
		// *******************************************************************
		// Part 3
		// Selection of the lines associated with full situations of a station
		// and
		// all its neighbor stations

		// Read the file containing the list of neighbors for each station
		JavaRDD<String> neighborsRDD = sc.textFile(inputPathNeighbors);

		// Map each line of the input file to a pair stationid, list of neighbor
		// stations
		JavaPairRDD<String, List<String>> nPairRDD = neighborsRDD.mapToPair(line -> {
			String[] fields = line.split(",");
			String stationId = fields[0];
			String[] neighbors = fields[1].split(" ");

			List<String> listNeighbors = new ArrayList<String>(Arrays.asList(neighbors));

			return new Tuple2<String, List<String>>(stationId, listNeighbors);
		});

		// Create a local HashMap object that will be used to store the
		// mapping stationid -> list of neighbors
		// There are 100 stations. Hence, data about neighbors can be stored in
		// main memory
		HashMap<String, List<String>> neighbors = new HashMap<String, List<String>>();

		for (Tuple2<String, List<String>> pair : nPairRDD.collect()) {
			neighbors.put(pair._1(), pair._2());
		}

		// Create a Broadcast version of the neighbors list
		final Broadcast<HashMap<String, List<String>>> neighborsBroadcast = sc.broadcast(neighbors);

		// The input data about the readings of the stations are already in
		// readingsRDD

		// Select the lines/readings associated with a full situation
		// (number of free slots equal to 0)
		JavaRDD<String> fullStatusLines = readingsRDD.filter(line -> {
			String[] fields = line.split(",");
			// fields[5] is the number of free slots

			if (Integer.parseInt(fields[5]) == 0)
				return true;
			else
				return false;
		});

		// Create a JavaPairRDD with key = timestamp and value=reading
		// associated with that timestamp
		JavaPairRDD<String, String> fullLinesPRDD = fullStatusLines.mapToPair(reading -> {

			String[] fields = reading.split(",");
			// The concatenation of fields[1], fields[2], fields[3] is the
			// timestamp
			String timestamp = new String(fields[1] + fields[2] + fields[3]);

			return new Tuple2<String, String>(timestamp, reading);
		});

		// Collapse all the values with the same key in one single pair
		// JavaPairRDD<String,String> fullLinesPRDD=
		JavaPairRDD<String, Iterable<String>> fullReadingsPerTimestamp = fullLinesPRDD.groupByKey();

		// Each pair contains a timestamp and the list of readings
		// (with number of free slots equal to 0) associated with that timestamp
		// Check, for each reading in the list, if all the neighbors of the
		// station of that reading are also present in this list of readings
		// Emit one record for each reading associated with a completely full
		// situation
		JavaRDD<String> selectedReadings = fullReadingsPerTimestamp
				.flatMap((Tuple2<String, Iterable<String>> timeStampReadings) -> {

					List<String> selectedReading = new ArrayList<String>();

					List<String> stations = new ArrayList<String>();

					// Extract the list of stations that appear in the readings
					// associated with the current key-value pair (i.e., the
					// list of stations
					// that are full in this timestamp)
					for (String reading : timeStampReadings._2()) {
						// Extract the stationid
						String fields[] = reading.split(",");
						String stationId = fields[0];

						stations.add(stationId);
					}

					// Iterate over the list of readings, i.e. iterate over the
					// list of string of the value of the key-value pair
					for (String reading : timeStampReadings._2()) {
						// This reading must be selected if all the neighbors of
						// the station of this reading are also in the value of
						// the current key-value pair
						// (i.e., if they are in stations)
						// Extract the stationid of this reading
						String fields[] = reading.split(",");
						String stationId = fields[0];

						// Select the list of its neighbors
						List<String> nCurrentStation = neighborsBroadcast.value().get(stationId);

						// check if all neighbors are in value (i.e., the local
						// variable stations) of the current key-value pair
						boolean allNeighborsFull = true;

						for (String neighborStation : nCurrentStation) {
							if (stations.contains(neighborStation) == false) {
								// There is at least one neighbor that is not in
								// the full status
								allNeighborsFull = false;
							}
						}

						if (allNeighborsFull == true) {
							selectedReading.add(reading);
						}
					}

					return selectedReading.iterator();
				});

		// Store the result in HDFS
		selectedReadings.saveAsTextFile(outputPath3);

		// Close the Spark context
		sc.close();
	}
}
