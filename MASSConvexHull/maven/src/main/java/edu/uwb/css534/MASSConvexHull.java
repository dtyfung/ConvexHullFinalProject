/*
  MASSConvexHull.java
  
  This is the convex hull application in MASS.
  
  Usage:
  To build: ~/apache-maven-3.6.0/bin/mvn package
  To clean: ~/apache-maven-3.6.0/bin/mvn clean
  Copy jar: cp ./target/prog5-1.0-SNAPSHOT.jar .
  Run app : java -jar prog5-1.0-SNAPSHOT.jar
  
  
 	MASS Java Software License
	© 2012-2015 University of Washington

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	The following acknowledgment shall be used where appropriate in publications, presentations, etc.:      

	© 2012-2015 University of Washington. MASS was developed by Computing and Software Systems at University of 
	Washington Bothell.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.

*/

package edu.uwb.css534;

import java.util.Date;
import java.io.PrintWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.awt.Point;
import java.lang.*;
import java.util.*;

import edu.uw.bothell.css.dsl.MASS.Agents;
import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Places;
import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;

public class MASSConvexHull {

	private static final String NODE_FILE = "nodes.xml";
	
	@SuppressWarnings("unused")		// some unused variables left behind for easy debugging
	public static void main( String[] args ) {
    
    if ( args.length < 3 ) {
      System.err.println("usage: MASSConvexHull <# of nodes> <# of points> <input file name>");
      System.exit(-1);
    }
    
    int nodes = Integer.parseInt(args[0]);
    int cores = 3;
    int pointCount = Integer.parseInt(args[1]);
    String filename = args[2];
    
		// remember starting time
		long startTime = new Date().getTime();
		
		// init MASS library
		MASS.setNodeFilePath( NODE_FILE );
		MASS.setLoggingLevel( LogLevel.DEBUG );
		
		// start MASS
		MASS.getLogger().debug( "MASSConvexHull initializing MASS library..." );
		MASS.init();
		MASS.getLogger().debug( "MASS library initialized" );
		
		int placeCount = nodes * cores;
	  
    // parses given file and stores into a list of list for each place
    Object[] dataList = readFile( filename, pointCount, placeCount );
    
    MASS.getLogger().debug( "dataList size: " + dataList.length );
    ArrayList<Point> list1 = (ArrayList<Point>)dataList[0];
        
		MASS.getLogger().debug( "Creating Places..." );
		Places places = new Places( 1, PointPlace.class.getName(), null, nodes, cores );
		MASS.getLogger().debug( "Places created" );
    
		// instruct all places to return the hostnames of the machines on which they reside
		MASS.getLogger().debug( "Sending callAll to Places..." );
		Object[] localHulls = ( Object[] ) places.callAll( PointPlace.COMPUTE_HULL, dataList );
		MASS.getLogger().debug( "Places callAll operation complete" );
		
    
    List<Point> res = new ArrayList<Point>();  
    for (int place = 0; place < placeCount; place++) {
      res.addAll((ArrayList<Point>)localHulls[place]);
    }
    
    System.out.println("Collected points: " + res.size());
    
    System.out.println("Computing global hull using graham scan");
    List<Point> globalHull = GrahamScan.getConvexHull(res);
    
    System.out.println("Global hull");
    //MASS.getLogger().debug("Global hull");
    for (Point p : globalHull) {
      System.out.println(p.x + "," + p.y);
      //MASS.getLogger().debug(p.x + "," + p.y);
    }
    
		// orderly shutdown
		MASS.getLogger().debug( "MASSConvexHull instructs MASS library to finish operations..." );
		MASS.finish();
		MASS.getLogger().debug( "MASS library has stopped" );
		
		// calculate / display execution time
		long execTime = new Date().getTime() - startTime;
		System.out.println( "Execution time = " + execTime + " milliseconds" );
		
	 }
   
   private static Object[] readFile(String filename, int pointCount, int placeCount) {
     System.out.println("In readfile");
     System.out.println("pointCount: " + pointCount);
     System.out.println("placeCount: " + placeCount);
     
     int partitionSize = pointCount/placeCount;
     boolean fullPartitionDone = false;
     
     System.out.println("partitionSize: " + partitionSize);
     
     Object[] dataSet = new Object[placeCount];  
     for (int i = 0; i < placeCount; i++) {
       dataSet[i] = (Object)(new ArrayList<Point>());
     }        
     
     List<Point> data = new ArrayList<Point>();
     
     try {  
       File file = new File( filename );
       Scanner input = new Scanner( file ).useDelimiter("\r\n"); 
       String str;
       int i = 0;
       int addedPoints = 0;
       
       while (input.hasNextLine() && !(str = input.nextLine()).isEmpty()) {
         String[] line = str.split("\\s");
         for ( int j = 0; j < line.length; j++ ) {              
          String[] s = line[j].split(",");
          
          ((ArrayList<Point>)dataSet[i]).add( new Point(Integer.parseInt(s[0]), Integer.parseInt(s[1])) );
          addedPoints++;
          
          // creates new data set when partition size is reached
          if ( addedPoints == partitionSize ) {
            i++;
            addedPoints = 0;
          }
          if ( i > dataSet.length - 1 && pointCount % placeCount != 0 ) fullPartitionDone = true;
          
          // allows residual points to be added to last partition
          if ( fullPartitionDone ) {
            i--;
            fullPartitionDone = false;
          }         
         }
       }
     } catch ( IOException e ) {
       e.printStackTrace(); 
     }
     return dataSet;
   }
}
 