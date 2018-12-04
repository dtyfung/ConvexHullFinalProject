/*
 * MRConvexHull.java
 *
 * Implementation is configured to a 4-node partition (cssmpi1 to cssmpi4).
 * Ideally would like to partition based on known nodes (difficult to find that out in code)
 * However can check using in bash: hadoop dfsadmin -report
 *
 * Usage:
 *   hadoop fs -rmr /user/<user name>/<hdfs output directory>
 *   javac -classpath ${HADOOP_HOME}/hadoop-${HADOOP_VERSION}-core.jar MRConvexHull.java
 *   jar -cvf mrconvexhull.jar *.class
 *   hadoop jar mrconvexhull.jar MRConvexHull <hdfs input directory> <hdfs output directory> <xRange> <yRange>
 *   hadoop fs -copyFromLocal <filename> /user/<user name>/<hdfs input directory>/.
 *   hadoop fs -copyToLocal /user/<user name>/<hdfs output directory>/part-00000 .
 */
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.awt.Point;

public class MRConvexHull {
  static final int NODES = 4; // partitions based on # of nodes
  
  public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>  {
 	  JobConf conf;
    public void configure( JobConf job ) {
      this.conf = job;
    }
	  
  	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, 
                                                 Reporter reporter) throws IOException {    
      // retrieve user arguments
      int xRange = Integer.parseInt(conf.get("xRange"));
      int yRange = Integer.parseInt(conf.get("yRange"));     
      int partitionSize = xRange / NODES;
      int[] startRange = new int[NODES];
      int[] endRange = new int[NODES]; 
      int[] partitions = new int[NODES];
      
      // set partition bounds
      for ( int node = 0; node < NODES; node++) {
        partitions[node] = xRange / NODES + ( ( node < xRange % NODES ) ? 1 : 0 );
        startRange[node] = (node == 0) ? 0 : (endRange[node - 1] + 1);
        endRange[node] = startRange[node] + partitions[node] - 1;
        System.out.println("node: " + node + " range[" + startRange[node] + "-" + endRange[node] + "]");
      }     
      
      String line = value.toString();
      String point;
      String[] coords = new String[2];
      StringTokenizer tokenizer = new StringTokenizer(line);
      int x;
      
      // parse file input   
      while (tokenizer.hasMoreTokens()) {
        point = tokenizer.nextToken();
        coords = point.split(",");
        x = Integer.parseInt( coords[0] );
        System.out.println("Read in point:(" + coords[0] + ", " + coords[1] + ")" );
        
        // partition points in terms of x-coordinate, then collects to output
        for ( int partition = 0; partition < NODES; partition++ ) {
          if ( x >= startRange[partition] && x <= endRange[partition] ) {
            output.collect( new Text( Integer.toString(partition) ), new Text( point ) );
            break;
          }    
        }
      }  
    }
  }
  
  public static class Reduce1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, 
                                                     Reporter reporter) throws IOException {
      String hullList = "";
      List<Point> points = new ArrayList<Point>(); 
      String[] coords; 
      while (values.hasNext()) {
     	 // split[0]: x, split[1]: y
	      coords = values.next( ).toString( ).trim().split(",");
        points.add( new Point( Integer.parseInt( coords[0] ), Integer.parseInt( coords[1] ) ) );  
      }
      
      for (Point pt : points) {
        System.out.println(pt.x + "," + pt.y);
      }
      
      // compute local hull
      List<Point> localHull = convexHull(points, points.size());      
      System.out.println("Printing out local hull points for partition " + key.toString());
      for (Point p : localHull) {
        hullList += p.x + "," + p.y + " ";
      }    
      System.out.println(hullList);
      output.collect(new Text( key ), new Text( hullList.trim() ));
      }
  }
  
  
  public static class Map2 extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
 	  JobConf conf;
    public void configure( JobConf job ) {
      this.conf = job;
    }
	  
  	public void map(Text key, Text value, OutputCollector<Text, Text> output, 
                                                 Reporter reporter) throws IOException {    
      String line = value.toString();
      String point;
      String[] coords = new String[2];
      StringTokenizer tokenizer = new StringTokenizer(line);
      int x;
      
      // parse file input, maps all sub hull points to a single key  
      while (tokenizer.hasMoreTokens()) {
        point = tokenizer.nextToken();
        coords = point.split(",");
        x = Integer.parseInt( coords[0] );
        System.out.println("Read in point:(" + coords[0] + ", " + coords[1] + ")" );
        output.collect( new Text( "global" ), new Text( point ) );
      }  
    }
  }
  
  public static class Reduce2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, 
                                                     Reporter reporter) throws IOException {
      String hullList = "";
      List<Point> points = new ArrayList<Point>(); 
      String[] coords; 
      while (values.hasNext()) {
     	 // split[0]: x, split[1]: y
	      coords = values.next( ).toString( ).trim().split(",");
        points.add( new Point( Integer.parseInt( coords[0] ), Integer.parseInt( coords[1] ) ) );  
      }
      
      for (Point pt : points) {
        System.out.println(pt.x + "," + pt.y);
      }
      
      // compute global hull
      List<Point> localHull = convexHull(points, points.size());      
      System.out.println("Printing out global hull");
      for (Point p : localHull) {
        hullList += "(" + p.x + ", " + p.y + ")\n";
      }    
      System.out.println(hullList);
      output.collect(key, new Text( hullList.trim() ));
    }
  }
  
  
  public static int orientation(Point p, Point q, Point r) 
  { 
        int val = (q.y - p.y) * (r.x - q.x) - 
                  (q.x - p.x) * (r.y - q.y); 
       
        if (val == 0) return 0;  // collinear 
        return (val > 0)? 1: 2; // clock or counterclock wise 
  } 
  
  // Prints convex hull of a set of n points. 
  public static List<Point> convexHull(List<Point> points, int n) 
  {  
      // Initialize Result 
      List<Point> hull = new ArrayList<Point>();
      
      // return and add points to global hull 
      if (n < 3) return points;  
      
      // Find the leftmost point 
      int l = 0; 
      for (int i = 1; i < n; i++) {
          if (points.get(i).x < points.get(l).x) 
              l = i; 
      }
      // Start from leftmost point, keep moving  
      // counterclockwise until reach the start point 
      // again. This loop runs O(h) times where h is 
      // number of points in result or output. 
      int p = l, q; 
      System.out.println("leftmost: " + l);
      int count = 0;
      do
      { 
          count++;
          if (count > 5000) break;
          // Add current point to result 
          hull.add(points.get(p)); 
          System.out.println("added " + points.get(p).x + "," + points.get(p).y);
     
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
             if (orientation(points.get(p), points.get(i), points.get(q)) == 2) 
                 q = i; 
          } 
     
          // Now q is the most counterclockwise with 
          // respect to p. Set p as q for next iteration,  
          // so that q is added to result 'hull' 
          p = q; 
          System.out.println("p: " + p);
      } while (p != l);  // While we don't come to first  
                         // point 
     
      return hull;
  } 
  
  public static void main(String[] args) throws Exception {
  	String tempDir = "temp/";
    JobConf conf1 = new JobConf(MRConvexHull.class);
  	conf1.setJobName("mrconvexhull1");
  	
  	conf1.setOutputKeyClass(Text.class);
  	conf1.setOutputValueClass(Text.class);
  	
  	conf1.setMapperClass(Map1.class);
  	//conf1.setCombinerClass(Reduce1.class);
  	conf1.setReducerClass(Reduce1.class);
  	
  	conf1.setInputFormat(TextInputFormat.class);
  	conf1.setOutputFormat(TextOutputFormat.class);
  	
  	FileInputFormat.setInputPaths(conf1, new Path(args[0]));
  	FileOutputFormat.setOutputPath(conf1, new Path(tempDir));
    
    //conf1.set( "argc", String.valueOf( args.length - 2 ) );
    conf1.set("xRange", args[2]);
    conf1.set("yRange", args[3]);
   
    JobConf conf2 = new JobConf(MRConvexHull.class);
  	conf2.setJobName("mrconvexhull2");
  	
  	conf2.setOutputKeyClass(Text.class);
  	conf2.setOutputValueClass(Text.class);
  	
  	conf2.setMapperClass(Map2.class);
  	//conf2.setCombinerClass(Reduce2.class);
  	conf2.setReducerClass(Reduce2.class);
  	
  	conf2.setInputFormat(KeyValueTextInputFormat.class);
  	conf2.setOutputFormat(TextOutputFormat.class);
  	
  	FileInputFormat.setInputPaths(conf2, new Path(tempDir));
  	FileOutputFormat.setOutputPath(conf2, new Path(args[1]));
    
    
    final long startTime = System.currentTimeMillis();
  	JobClient.runJob(conf1);
    JobClient.runJob(conf2);
    final long endTime = System.currentTimeMillis();
    
    System.out.println("Total execution time (MapReduce Convex Hull, 4 node): " + (endTime - startTime));
  }
}
