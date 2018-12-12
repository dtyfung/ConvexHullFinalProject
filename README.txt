To run the project:

MapReduce

1. Copy all source files from /MapReduceConvexHull (Using hadoop version 0.20.2).
2. a) hadoop fs -rmr /user/<user name>/<hdfs output directories, must include "temp/">
   b) javac -classpath ${HADOOP_HOME}/hadoop-${HADOOP_VERSION}-core.jar MRConvexHull.java
   c) jar -cvf mrconvexhull.jar *.class
   d) hadoop jar mrconvexhull.jar MRConvexHull <hdfs input directory> <hdfs output directory> <xRange> <yRange>
   e) hadoop fs -copyFromLocal <filename> /user/<user name>/<hdfs input directory>/.
   f) hadoop fs -copyToLocal /user/<user name>/<hdfs output directory>/part-00000 .

Spark

1. Copy all source file from /SparkConvexHull or just copy SparkConvexHull.jar to the directory that you will run the program.
2. Create point data by using Generator.java in folder /PointGenerator, or you can use preset data points in /TestData
   java Gernerator <# of points> <# of point for each line> <xRange> <yRange> <filename>
3. Copy testing data to the directory that you will run the program. 
4. to run spark spark-submit --class SparkConvexHull --master spark://cssmpi1.uwb.edu:16766 --executor-memory 2G --total-executor-cores 12 SparkConvexHull.jar <name of the data file>

MASS

1. Copy all source files from /MASSConvexHull 
2. a) To build: ~/apache-maven-3.6.0/bin/mvn package
   b) To clean: ~/apache-maven-3.6.0/bin/mvn clean
   c) Copy jar: cp ./target/prog5-1.0-SNAPSHOT.jar .
   d) Run app : java -jar prog5-1.0-SNAPSHOT.jar <# nodes> <# of points> <input file path>
