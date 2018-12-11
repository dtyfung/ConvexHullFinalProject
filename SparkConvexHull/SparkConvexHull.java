import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.lang.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.awt.Point;

public class SparkConvexHull {

    public static void main(String[] args) {
            // start Sparks and read a given input file
        if (args.length != 1) {
            System.out.println("Please input valid name of txt file.");
        }
        String inputFile = args[0];

        SparkConf conf = new SparkConf( ).setAppName( "Convex Hall Computation" );
        JavaSparkContext jsc = new JavaSparkContext( conf );
        JavaRDD<String> lines = jsc.textFile( inputFile );
        // now start a timer
        long startTime = System.currentTimeMillis();

        JavaRDD<Point> pointSet = lines.flatMap( line -> { 
            line.trim();
            String[] buff = line.split(" ");
            List<Point> list = new ArrayList<Point>();
           
            // split each point and create new point objects
            for (String current : buff) {
                if(current.equals("")) {
                    continue;
                }
                String[] temp = current.split(","); 
                int x = Integer.parseInt(temp[0]);
                int y = Integer.parseInt(temp[1]);
                list.add(new Point(x, y));
            }
            return list.iterator();
        } );

        // Compute local convex hall for each part of rdd.
        JavaRDD<Point> localReduced = pointSet.mapPartitions( theIterator -> {
            // extract all point from iterator to a list
            List<Point> list = new ArrayList<Point>(); 
            
            while(theIterator.hasNext()) {
			    Point p = theIterator.next();
			    list.add(p);
            } 
            
            List<Point> res = GrahamScan.getConvexHull(list);

            return res.iterator();
        } );

        List<Point> templ = localReduced.collect();
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + count);
    
        // Merge all RDD into 1
        JavaRDD<Point> global = jsc.parallelize(temp, 1);

        // Compute Global convex hall for each part of rdd.
        JavaRDD<Point> globalReduced = global.mapPartitions( theIterator -> {
            // extract all point from iterator to a list
            List<Point> list = new ArrayList<Point>(); 
            
            while(theIterator.hasNext()) {
			    Point p = theIterator.next();
			    list.add(p);
            } 
            
            List<Point> res = GrahamScan.getConvexHull(list);
            return res.iterator();
        } );

        List<Point> res = globalReduced.collect();
        long endTime = System.currentTimeMillis();

        System.out.println("Elapsed time = " + (endTime - startTime));
        System.out.println("Number of Convex Hall points: " + res.size());
        for(Point current : res) {
            System.out.println("(" + current.x + ", " + current.y + "); ");
        }
        jsc.stop();
    }
}