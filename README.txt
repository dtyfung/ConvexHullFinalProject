To run the project:




Spark

1. Copy all source file form /SparkConvexHull or just copy SparkConvexHull.jar to the directory that you will run the program.
2. Creat point data by using Generator.java in folder /PointGenerator, or you can use preset data points in /TestData
   java Gernerator <# of points> <# of point for each line> <xRange> <yRange> <filename>
3. Copy testing data to the directory that you will run the program. 
4. to run spark spark-submit --class SparkConvexHull --master spark://cssmpi1.uwb.edu:16766 --executor-memory 2G --total-executor-cores 12 SparkConvexHull.jar <name of the data file>