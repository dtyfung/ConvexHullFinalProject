/*
  MASSConvexHull.java
  
  This is the convex hull application.
  
  
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
		
		/* 
		 * Create all Places (having dimensions of x, y, and z)
		 * ( the total number of Place objects that will be created is: x * y * z )
		 */
		int placeCount = nodes * cores;
	  
    // parses given file and stores into a list of list for each place
    Object[] dataList = readFile( filename, pointCount, placeCount );
    
		MASS.getLogger().debug( "Creating Places..." );
		Places places = new Places( 1, PointPlace.class.getName(), null, placeCount );
		MASS.getLogger().debug( "Places created" );
    
    /*
    // initialize all places to have their own data set
    MASS.getLogger().debug("Sending data to places...");
    places.callAll( PointPlace.PLACE_INIT, dataList );
    MASS.getLogger().debug("All places received data sets");
    */
    
		// instruct all places to return the hostnames of the machines on which they reside
		MASS.getLogger().debug( " Sending callAll to Places..." );
		Object[] localHulls = ( Object[] ) places.callAll( PointPlace.COMPUTE_HULL, dataList );
		MASS.getLogger().debug( "Places callAll operation complete" );
		
  
    List<Point> res = new ArrayList<Point>();  
    for (int place = 0; place < placeCount; place++) {
      res.addAll((ArrayList<Point>)localHulls[place]);
    }
    
    List<Point> globalHull = ConvexHull.computeHull(res);
    
    MASS.getLogger().debug("Global hull;");
    for (Point p : globalHull) {
      MASS.getLogger().debug(p.x + "," + p.y);
    }
    /*
		// create Agents (number of Agents = x * y in this case), in Places
		MASS.getLogger().debug( "Quickstart creating Agents..." );
		Agents agents = new Agents( 1, Nomad.class.getName(), null, places, x * y );
		MASS.getLogger().debug( "Agents created" );
    */
    
    /*
		// instruct all Agents to return the hostnames of the machines on which they reside
		Object[] agentsCallAllObjs = new Object[ x * y ];
		MASS.getLogger().debug( "Quickstart sending callAll to Agents..." );
		Object[] calledAgentsResults = ( Object[] ) agents.callAll( Nomad.GET_HOSTNAME, agentsCallAllObjs );
		MASS.getLogger().debug( "Agents callAll operation complete" );
		
		// move all Agents across the Z dimension to cover all Places
		for (int i = 0; i < z; i ++) {
			
			// tell Agents to move
			MASS.getLogger().debug( "Quickstart instructs all Agents to migrate..." );
			agents.callAll(Nomad.MIGRATE);
			MASS.getLogger().debug( "Agent migration complete" );
			
			// sync all Agent status
			MASS.getLogger().debug( "Quickstart sending manageAll to Agents..." );
			agents.manageAll();
			MASS.getLogger().debug( "Agents manageAll operation complete" );
			
			// find out where they live now
			MASS.getLogger().debug( "Quickstart sending callAll to Agents..." );
			calledAgentsResults = ( Object[] ) agents.callAll( Nomad.GET_HOSTNAME, agentsCallAllObjs );
			MASS.getLogger().debug( "Agentcalls callAll operation complete" );
			
		}
		
		// find out where all of the Agents wound up when all movements complete
		MASS.getLogger().debug( "Quickstart sending callAll to Agents to get final landing spot..." );
		calledAgentsResults = ( Object[] ) agents.callAll(Nomad.GET_HOSTNAME, agentsCallAllObjs );
		MASS.getLogger().debug( "Agents callAll operation complete" );
		*/
		// orderly shutdown
		MASS.getLogger().debug( "Quickstart instructs MASS library to finish operations..." );
		MASS.finish();
		MASS.getLogger().debug( "MASS library has stopped" );
		
		// calculate / display execution time
		long execTime = new Date().getTime() - startTime;
		System.out.println( "Execution time = " + execTime + " milliseconds" );
		
	 }
   
   private static Object[] readFile(String filename, int pointCount, int placeCount) {
     int partitionSize = pointCount/placeCount;
     Object[] dataSet = new Object[placeCount];     
     
     List<Point> data = new ArrayList<Point>();
     int k = 0;
     
     try {  
       File file = new File( filename );
       Scanner input = new Scanner( file ).useDelimiter("\r\n"); 
       String str;
       int i = 0;
       int addedPoints = 0;
       
       while (!(str = input.nextLine()).isEmpty()) {
         String[] line = str.split("\\s");
         for ( int j = 0; j < line.length; j++ ) {              
          String[] s = line[j].split(",");
          // TODO: don't create new data list if k is last element in dataSet
          if (addedPoints % partitionSize == 0) {
            dataSet[k] = data;
            data = new ArrayList<Point>();
            k++;
          }
          data.add( new Point(Integer.parseInt(s[0]), Integer.parseInt(s[1])) );
          addedPoints++;
          i++;
         }
       }
       MASS.getLogger().debug("Added " + addedPoints + " points to dataset.");
       MASS.getLogger().debug("Parsed input file: " + filename + ".");
       
     } catch ( IOException e ) {
       e.printStackTrace(); 
     }
     return dataSet;
   }
}
