package it.polito.bigdata.spark.exercise44;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.ArrayList;

import org.apache.spark.SparkConf;

public class SparkDriver {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		String inputPathWatched;
		String inputPathPreferences;
		String inputPathMovies;
		String outputPath;
		double threshold;

		inputPathWatched = args[0];
		inputPathPreferences = args[1];
		inputPathMovies = args[2];
		outputPath = args[3];
		threshold = Double.parseDouble(args[4]);

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #44");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the watched movies file
		JavaRDD<String> watchedRDD = sc.textFile(inputPathWatched);

		// Select only the userid and the movieid
		// Define a JavaPairRDD with movieid as key and userid as value
		JavaPairRDD<String, String> movieUserPairRDD = watchedRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> movieUser = new Tuple2<String, String>(fields[1], fields[0]);

			return movieUser;
		});

		// Read the content of the movies file
		JavaRDD<String> moviesRDD = sc.textFile(inputPathMovies);

		// Select only the movieid and genre
		// Define a JavaPairRDD with movieid as key and genre as value
		JavaPairRDD<String, String> movieGenrePairRDD = moviesRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> movieGenre = new Tuple2<String, String>(fields[0], fields[2]);

			return movieGenre;
		});

		// Join watched movie with movies
		JavaPairRDD<String, Tuple2<String, String>> joinWatchedGenreRDD = movieUserPairRDD.join(movieGenrePairRDD);

		// Select only userid (as key) and genre (as value)
		JavaPairRDD<String, String> usersWatchedGenresRDD = joinWatchedGenreRDD
				.mapToPair((Tuple2<String, Tuple2<String, String>> userMovie) -> {
					// movieid - userid - genre
					Tuple2<String, String> movieGenre = new Tuple2<String, String>(userMovie._2()._1(),
							userMovie._2()._2());

					return movieGenre;
				});

		// Read the content of the preferences
		JavaRDD<String> preferencesRDD = sc.textFile(inputPathPreferences);

		// Define a JavaPairRDD with userid as key and genre as value
		JavaPairRDD<String, String> userLikedGenresRDD = preferencesRDD.mapToPair(line -> {

			String[] fields = line.split(",");
			Tuple2<String, String> userGenre = new Tuple2<String, String>(fields[0], fields[1]);

			return userGenre;
		});

		// Cogroup the lists of watched and liked genres for each user
		// There is one pair for each userid
		// the value contains the list of genres (with repetitions) of the
		// watched movies and
		// the list of liked genres
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> userWatchedLikedGenres = usersWatchedGenresRDD
				.cogroup(userLikedGenresRDD);

		// Filter the users with a misleading profile
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> misleadingUsersListsRDD = userWatchedLikedGenres
				.filter((Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> listWatchedLikedGenres) -> {

					// Store in a local list the "small" set of liked genres
					// associated with the current user
					ArrayList<String> likedGenres = new ArrayList<String>();

					for (String likedGenre : listWatchedLikedGenres._2()._2()) {
						likedGenres.add(likedGenre);
					}

					// Count
					// - The number of watched movies for this user
					// - How many of watched movies are associated with a liked
					// genre
					int numWatchedMovies = 0;
					int notLiked = 0;

					for (String watchedGenre : listWatchedLikedGenres._2()._1()) {
						numWatchedMovies++;

						if (likedGenres.contains(watchedGenre) == false) {
							notLiked++;
						}
					}

					// Check if the number of watched movies associated with a non-liked genre
					// is greater that threshold%
					if ((double) notLiked > threshold * (double) numWatchedMovies) {
						return true;
					} else
						return false;
				});

		// Select only the userid of the users with a misleading profile
		JavaRDD<String> misleadingUsersRDD = misleadingUsersListsRDD.keys();

		misleadingUsersRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
