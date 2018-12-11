/*
 * MRConvexHull.java
 *
 * Implementation is configured to a 4-node, 12 core partition (cssmpi1 to cssmpi4).
 * Ideally would like to partition based on known PARTITIONS (difficult to find that out in code)
 * However can check using in bash: hadoop dfsadmin -report
 *
 * Usage:
 *   hadoop fs -rmr /user/<user name>/<hdfs output directories>
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
  static final int PARTITIONS = 12; // partitions based on # of cpu cores
  
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
      int partitionSize = xRange / PARTITIONS;
      int[] startRange = new int[PARTITIONS];
      int[] endRange = new int[PARTITIONS]; 
      int[] partitions = new int[PARTITIONS];
      
      // set partition bounds
      for ( int node = 0; node < PARTITIONS; node++) {
        partitions[node] = xRange / PARTITIONS + ( ( node < xRange % PARTITIONS ) ? 1 : 0 );
        startRange[node] = (node == 0) ? 0 : (endRange[node - 1] + 1);
        endRange[node] = startRange[node] + partitions[node] - 1;
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
        
        // partition points in terms of x-coordinate, then collects to output
        for ( int partition = 0; partition < PARTITIONS; partition++ ) {
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
      for (Point p : localHull) {
        hullList += p.x + "," + p.y + " ";
      }    
      output.collect(new Text( key ), new Text( hullList.trim() ));
      }
  }
  
  // Mapper class that maps all local hull data into a singular global key
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
        output.collect( new Text( "global" ), new Text( point ) );
      }  
    }
  }
  
  // Reducer class that computes the final global hull
  public static class Reduce2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, 
                                                     Reporter reporter) throws IOException {
      String hullList = "\n";
      List<Point> points = new ArrayList<Point>(); 
      String[] coords; 
      while (values.hasNext()) {
     	 // split[0]: x, split[1]: y
	      coords = values.next( ).toString( ).trim().split(",");
        points.add( new Point( Integer.parseInt( coords[0] ), Integer.parseInt( coords[1] ) ) );  
      }
      
      // compute global hull
      List<Point> localHull = convexHull(points, points.size());      
      for (Point p : localHull) {
        hullList += "(" + p.x + ", " + p.y + ")\n";
      }    
      output.collect(key, new Text( hullList.trim() ));
    }
  }
  
  /*********************************** Jarvis march algorithm *********************************/
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
      do
      { 
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
  	conf1.setReducerClass(Reduce1.class);
  	
  	conf1.setInputFormat(TextInputFormat.class);
  	conf1.setOutputFormat(TextOutputFormat.class);
  	
  	FileInputFormat.setInputPaths(conf1, new Path(args[0]));
  	FileOutputFormat.setOutputPath(conf1, new Path(tempDir));
    
    conf1.set("xRange", args[2]);
    conf1.set("yRange", args[3]);
   
    JobConf conf2 = new JobConf(MRConvexHull.class);
  	conf2.setJobName("mrconvexhull2");
  	
  	conf2.setOutputKeyClass(Text.class);
  	conf2.setOutputValueClass(Text.class);
  	
  	conf2.setMapperClass(Map2.class);
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
