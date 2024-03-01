Problem Detals: Misleading profile selection

▪ Input: 
A textual file containing the list of movies watched by the users of a video on demand service
Each line of the file contains the information about one visualization
(userid,movieid,start-timestamp,end-timestamp)
The user with id userid watched the movie with id movieid from start-timestamp to end-timestamp

▪ Input2: 
A second textual file containing the list of preferences for each user
Each line of the file contains the information about one preference
(userid,movie-genre)
The user with id userid liked the movie of type movie-genre

▪ Input3: 
A third textual file containing the list of movies with the associated information
Each line of the file contains the information about one movie
(movieid,title,movie-genre)
There is only one line for each movie i.e., each movie has one single genre

▪ Outputs:
Select the userids of the list of users with a misleading profile
A user has a misleading profile if more than threshold% of the movies he/she watched are not associated with a movie genre he/she likes
threshold is an argument/parameter of the application and it is specified by the user
Store the result in an HDFS file


We have an interface to connect to HDFS server.
The interface is https://jupyter.polito.it/. It is our Gateway Server to connect BigData@Polito cluster.
We have an access to Reserved 1 CPU threads/8 GB mem, max 8 CPU threads and 24 GB memory.

After logining to the Jupyter Interface we have to upload our Jar file and InputFile to it.
Then we have to use command line approach or just run the bash file(run.sh).