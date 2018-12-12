#!/bin/sh

javac -cp /home/kaihuh_css534/spark-2.3.1-bin-hadoop2.7/jars/scala-library-2.11.8.jar:/home/kaihuh_css534/spark-2.3.1-bin-hadoop2.7/jars/spark-core_2.11-2.3.1.jar:/home/kaihuh_css534/spark-2.3.1-bin-hadoop2.7/jars/spark-sql_2.11-2.3.1.jar:. SparkConvexHull.java GrahamScan.java

jar -cvf SparkConvexHull.jar SparkConvexHull.class GrahamScan.class
