/*
  PointPlace.java
  
  This is the Convex Hull Place class.
  
  
   
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

import java.net.InetAddress;
import java.util.*;
import java.awt.Point;
import edu.uw.bothell.css.dsl.MASS.Place;

public class PointPlace extends Place {

	public static final int GET_HOSTNAME = 0;
  //public static final int PLACE_INIT   = 1;
	public static final int COMPUTE_HULL = 1;	
 
  private List<Point> points;
	//private size;
  private List<Point> result;
  
  /**
	 * This constructor will be called upon instantiation by MASS
	 * The Object supplied MAY be the same object supplied when Places was created
	 * @param obj
	 */
	public PointPlace(Object obj) { }
    
	/**
	 * This method is called when "callAll" is invoked from the master node
	 */
	public Object callMethod(int method, Object o) {
		
		switch (method) {
		
		case GET_HOSTNAME:
			return findHostName(o);
		
    /*
    case PLACE_INIT;
      return initialize(o);
    */
    case COMPUTE_HULL:
      return convexHull(o);
      
		default:
			return new String("Unknown Method Number: " + method);
		
		}	
	}
	
	/**
	 * Return a String identifying where this Place is actually located
	 * @param o
	 * @return The hostname (as a String) where this Place is located
	 */
	public Object findHostName(Object o){
		
		try{
			return (String) "Place located at: " + InetAddress.getLocalHost().getCanonicalHostName() +" " + Integer.toString(getIndex()[0]) + ":" + Integer.toString(getIndex()[1]) + ":" + Integer.toString(getIndex()[2]);
        }
		
		catch (Exception e) {
			return "Error : " + e.getLocalizedMessage() + e.getStackTrace();
		}
    
	}
 
  public Object initialize(List<Point> points) { 
    this.points = points;
    return null;
  }

  private int orientation(Point p, Point q, Point r) 
  { 
        int val = (q.y - p.y) * (r.x - q.x) - 
                  (q.x - p.x) * (r.y - q.y); 
       
        if (val == 0) return 0;  // collinear 
        return (val > 0)? 1: 2; // clock or counterclock wise 
  }  
  
  public Object convexHull(Object o) {
    return ConvexHull.computeHull( (ArrayList<Point>)points );
  }
  
  /*
  // Computes a convex hull of a set of n points. 
  public static List<Point> convexHull() 
  {  
      // return and add points to global hull 
      if (this.n < 3) this.result = this.points;
      
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
          result.add(points.get(p)); 
     
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
      } while (p != l);  // While we don't come to first  
                         // point 
      return null;
  }
  */
  public List<Point> getResult() {
    return this.result;
  } 
  
}

