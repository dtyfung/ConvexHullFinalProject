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
		public int totalPts, xRange, yRange;
		public String name;
		int length;
    HashSet<Point> set;
    
    public static void main( String[] args ) {
    	if ( args.length != 4 ) {
    	    System.out.println( "usage: java Generator <# of points> <xRange> <yRange> <filename>" );
    	    System.exit( -1 );
			}
			
			Generator temp = new Generator(args);

      if ( temp.totalPts >  temp.xRange *  temp.yRange ) {
        System.out.println("# of points cannot be more than xRange * yRange");
        System.exit( -1 );
			}
			
			temp.Creation();
    }

    public Generator( String[] args ) {
				this.set = new HashSet<Point>();
				this.totalPts = Integer.parseInt( args[0] );
				this.xRange = Integer.parseInt( args[1] );
				this.yRange = Integer.parseInt( args[2] );
				this.name = args[3];
				this.length = args.length;
		}
		
		public void Creation() {
				int remainingPts = totalPts;
				Random rand = new Random();
				try {
					PrintWriter file = ( length == 4 ) ? new PrintWriter( name ) : null;
					Point p;
					
					while ( remainingPts > 0 ) {
						p = new Point( Math.abs(rand.nextInt() % xRange), Math.abs(rand.nextInt() % yRange) );
						if ( file != null && set.add( p ) ) {
								file.print( p.x + "," + p.y + " " ); 
									remainingPts--;
						}
					}
				file.println();
					if ( file != null ) {
						file.close( );
						System.out.println( "file: " + name + " was created" );
					}
				} catch( IOException e ) {
						e.printStackTrace( );
				}
		}	
}
