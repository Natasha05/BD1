# Wikipedia History Analysis using Apache Hadoop Libraries


Count the number of changes done for an article in the given input time interval.


At first created two jobs to process the query. 
The first job was to produce the file with key(article id) and value(count of modifications) and the second job was to sort the output produced by first job. But the program was taking longer to run.


To optimize the program created a new class to read the temp o/p file from reducer to sort the data and write to another o/p file. Keeping another file also slower down the process. But as rather than using file, HashMap and List in reducer speed up the processing So, I used the HashMap and List in reducer. First I dump the data into HashMap and then override the cleanup() function in reducer. Implement sorting and context write in cleanup(). Resultant I got faster response but I do think that the program can be optimized more to improve the performance. 



After performing multiple test I noticed that there is not much difference in processing time if you only change the K value but when you use a longer interval then query processing time increases as well as bytes transferred over n/w.
