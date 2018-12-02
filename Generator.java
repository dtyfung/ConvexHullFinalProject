/* 
 * Generator.java
 * 
 * Creates a set of coordinate points and saves it into a txt file given
 *
 * 
 * javac Generator.java
 * java Generator 10 50 50 data.txt
 */
import java.util.*;
import java.io.PrintWriter;
import java.io.IOException;
import java.awt.Point;
import java.util.HashSet;
import java.lang.*;

public class Generator {
    int totalPts, xRange, yRange;
    HashSet<Point> set;
    
    public static void main( String[] args ) {
    	if ( args.length != 4 ) {
    	    System.out.println( "usage: java Generator <# of points> <xRange> <yRange> <filename>" );
    	    System.exit( -1 );
    	}
      if ( args[0] > args[1] * args[2] ) {
        System.out.println("# of points cannot be more than xRange * yRange");
        System.exit( -1 );
      }
    	Generator g = new Generator( args );
    }

    public Generator( String[] args ) {
      this.set = new HashSet<Point>();
    	this.totalPts = Integer.parseInt( args[0] );
      this.xRange = Integer.parseInt( args[1] );
      this.yRange = Integer.parseInt( args[2] );
      int remainingPts = totalPts;
      Random rand = new Random();
      
    	try {
  	    PrintWriter file = ( args.length == 4 ) ? new PrintWriter( args[3] ) : null;
        Point p;
        
        while ( remainingPts > 0 ) {
      		p = new Point( Math.abs(rand.nextInt() % xRange), Math.abs(rand.nextInt() % yRange) );
      		if ( file != null && set.add( p ) ) {
      		    file.print( p.x + "," + p.y + " " ); 
      		      remainingPts--;
  		    }
	      }
  	    if ( file != null ) {
      		file.close( );
      		System.out.println( "file: " + args[3] + " was created" );
  	    }
    	} catch( IOException e ) {
    	    e.printStackTrace( );
    	}
   }
}
