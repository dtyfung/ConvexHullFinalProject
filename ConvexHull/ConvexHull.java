/* 
 * ConvexHull.java
 *
 * Sequential Convex Hull computation using the Gift Wrapping/Jarvis algorithm. 
 * Source: https://www.geeksforgeeks.org/convex-hull-set-1-jarviss-algorithm-or-wrapping/
 * 
 * // input file assumed to be in same directory
 * java ConvexHull <# of points> <input filename> <output filename> 
 * input file format  : x1,y1 x2,y2 x3,y3 x4,y4 ... xn,yn
 * output file format : (x1,y1) \n (x2,y2) \n (x3,y3) \n ... \n (xn,yn)
 *
 */
import java.util.*;
import java.io.PrintWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.awt.Point;
import java.lang.*;

class ConvexHull { 
      
    // To find orientation of ordered triplet (p, q, r). 
    // The function returns following values 
    // 0 --> p, q and r are colinear 
    // 1 --> Clockwise 
    // 2 --> Counterclockwise 
    public static int orientation(Point p, Point q, Point r) 
    { 
        int val = (q.y - p.y) * (r.x - q.x) - 
                  (q.x - p.x) * (r.y - q.y); 
       
        if (val == 0) return 0;  // collinear 
        return (val > 0)? 1: 2; // clock or counterclock wise 
    } 
      
    // Prints convex hull of a set of n points. 
    public static List<Point> convexHull(Point points[], int n) 
    {  
        // Initialize Result 
        List<Point> hull = new ArrayList<Point>();
        
        // There must be at least 3 points 
        if (n < 3) {
          System.out.println("Need at least 3 data points");
          return hull;  
        }
        
        // Find the leftmost point 
        int l = 0; 
        for (int i = 1; i < n; i++) 
            if (points[i].x < points[l].x) 
                l = i; 
       
        // Start from leftmost point, keep moving  
        // counterclockwise until reach the start point 
        // again. This loop runs O(h) times where h is 
        // number of points in result or output. 
        int p = l, q; 
        do
        { 
            // Add current point to result 
            hull.add(points[p]); 
       
            // Search for a point 'q' such that  
            // orientation(p, x, q) is counterclockwise  
            // for all points 'x'. The idea is to keep  
            // track of last visited most counterclock- 
            // wise point in q. If any point 'i' is more  
            // counterclock-wise than q, then update q. 
            q = (p + 1) % n; 
              
            for (int i = 0; i < n; i++) 
            { 
               // If i is more counterclockwise than  
               // current q, then update q 
               if (orientation(points[p], points[i], points[q]) 
                                                   == 2) 
                   q = i; 
            } 
       
            // Now q is the most counterclockwise with 
            // respect to p. Set p as q for next iteration,  
            // so that q is added to result 'hull' 
            p = q; 
       
        } while (p != l);  // While we don't come to first  
                           // point 
       
        return hull;
    } 
      
    /* Driver program to test above function */
    public static void main(String[] args)  
    { 
        if ( args.length != 3 || Integer.parseInt(args[0]) < 0 ) {
          System.out.println("usage: java ConvexHull <# of points> <input file name> <output file name>");
          System.exit( -1 );
        }
        final long startTime = System.currentTimeMillis();
        Point[] points = new Point[Integer.parseInt( args[0] )]; 
        
        try {
          File file = new File( args[1] );
          Scanner input = new Scanner( file ).useDelimiter("\r\n"); 
          String str;
          int i = 0;
          while (!(str = input.nextLine()).isEmpty()) {
            String[] line = str.split("\\s");
            for ( int j = 0; j < line.length; j++ ) {              
             String[] s = line[j].split(",");
             points[i] = new Point(Integer.parseInt(s[0]), Integer.parseInt(s[1]));
             i++;
            }
          }
          int n = points.length; 
          
   	      List<Point> hull = convexHull(points, n); 
          
          // Print Result 
          PrintWriter output = new PrintWriter( args[2] );
          for (Point temp : hull) 
              output.println("(" + temp.x + ", " + temp.y + ")");  
          output.close( );
          System.out.println("output file: " + args[2] + " created.");
          final long endTime = System.currentTimeMillis();
          System.out.println("Sequential Convex Hull (" + n + ") points took " + (endTime - startTime) + " msec."); 
        
        } catch ( IOException e ) {
          e.printStackTrace(); 
        }
    } 
} 