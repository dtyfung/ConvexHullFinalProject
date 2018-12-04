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

        // boolean firstTime = ture;
        // while (pointSet.count() >= 400) {

        //     if (firstTime == false) {
        //         pointSet.repartition(4);
        //     }
        //     // Compute local convex hall for each part of rdd.
        //     pointSet.mapPartitions( theIterator -> {
        //         // extract all point from iterator to a list
        //         List<Point> list = new ArrayList<Point>(); 
                
        //         while(theIterator.hasNext()) {
        //             Point p = theIterator.next();
        //             list.add(p);
        //         } 
                
        //         List<Point> res = convexHull(list, list.size());
        //         return res.iterator();
        //     } );

        // }


        // Compute local convex hall for each part of rdd.
        JavaRDD<Point> localReduced = pointSet.mapPartitions( theIterator -> {
            // extract all point from iterator to a list
            List<Point> list = new ArrayList<Point>(); 
            
            while(theIterator.hasNext()) {
			    Point p = theIterator.next();
			    list.add(p);
            } 
            
            List<Point> res = convexHull(list, list.size());

            return res.iterator();
        } );

        List<Point> local = localReduced.collect();
        System.out.println("Number of Convex Hall points: " + local.size());
        for (Point current : local) {
             System.out.println("(" + current.x + ", " + current.y + ") ; ");
         }

        // Merge all RDD into 1
        JavaRDD<Point> global = localReduced.repartition(1);

        // Compute Global convex hall for each part of rdd.
        JavaRDD<Point> globalReduced = global.mapPartitions( theIterator -> {
            // extract all point from iterator to a list
            List<Point> list = new ArrayList<Point>(); 
            
            while(theIterator.hasNext()) {
			    Point p = theIterator.next();
			    list.add(p);
            } 
            
            List<Point> res = convexHull(list, list.size());
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

//---------------------------Jarvis’s Algorithm-----------------------------------------
    public static int orientation(Point p, Point q, Point r) 
    { 
        int val = (q.y - p.y) * (r.x - q.x) - (q.x - p.x) * (r.y - q.y); 
       
        if (val == 0) {
            return 0;  // collinear 
        } else {
            return (val > 0)? 1: 2; // 1 -> clock or 2 -> counterclock wise 
        }
    } 

    //Compute convex hull of a set of n points.
    public static List<Point> convexHull(List<Point> list, int n) 
    { 
        // Initialize Result 
        List<Point> res = new ArrayList<Point>(); 

        // There must be at least 3 points 
        if (n < 3) return list; 
       
        // Find the leftmost point 
        int l = 0; 
        for (int i = 1; i < n; i++) {
            if (list.get(i).x < list.get(l).x) {
                l = i; 
            }
       
        }       

        int p = l, q; 

        // While don't come back to first point 
        do {  
            // Add current point to result 
            res.add(list.get(p)); 
       
            q = (p + 1) % n; 
                      
            for (int i = 0; i < n; i++) 
            { 
               // If i is more counterclockwise than  
               // current q, then update q 
               if (orientation(list.get(p), list.get(i), list.get(q)) == 2) {
                    q = i; 
               }
            } 
       
            // Now q is the most counterclockwise with 
            p = q; 
        } while (p != l);
        
        return res;

    }     
}