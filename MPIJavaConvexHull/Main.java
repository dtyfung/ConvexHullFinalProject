import mpi.*;

import java.awt.*;
import java.awt.geom.Line2D;
import java.io.*;
import java.io.File;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.awt.Polygon;
import java.awt.geom.Line2D;
import java.awt.geom.PathIterator;
import java.awt.geom.Point2D;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class Main {
    public static void main(String[] args) throws Exception {
        MPI.Init(args);

        int myrank = MPI.COMM_WORLD.getRank();
        int size = MPI.COMM_WORLD.getSize() ;
        System.out.println("Hello world from rank " + myrank + " of " + size);



        // start measuring time
        final long startTime = System.currentTimeMillis();
        ArrayList<Point> points = new ArrayList<>();

        try(BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
            br.lines().forEach(line -> Arrays.stream(line.split(" ")).map(aLine -> aLine.split(",")).map(s -> new Point(Integer.parseInt(s[0]), Integer.parseInt(s[1]))).forEach(points::add));
        }

        // sort along x axis
        points.sort(Comparator.comparingInt(x -> x.x));


        // each rank starts task
        if (myrank != 0 )
            splitPerform(myrank, size, points);
        if (myrank == 0 ) {
            receiveAndJoinHulls(points.size(), size, "johar.txt", startTime, points);
        }

        MPI.Finalize();



    }

    static void splitPerform(int rank, int numRanks, ArrayList<Point> points) throws MPIException {
        // split points for each rank to process
        int myBegin = (int) ((double)rank*((double)points.size()/(double)numRanks));
        int myEnd = (int) (myBegin+((double)points.size()/(double) numRanks));
        Point[] store = points.subList(myBegin, myEnd).toArray(new Point[0]);
        List<Point> hull = ConvexHull.convexHull(store, store.length);

        Point[] hulle = new Point[hull.size()+1];
        int j = 0;
        for (int i = 0; i < hull.size(); i++) {
            if (j < hull.size()) hulle[i] = hull.get(i);
            else hulle[i] = new Point(-1, -1);
            j++;

        }
        hulle[hull.size()] = new Point(-1, -1);

        // send to rank 0 to compute overall convex hull
        sendHull(hulle, points.size());

    }
    //TODO: Test send and receive by sending entire array from ranks and combining ar rank 0 then compare if same as original
    static void sendHull(Point[] hull, int size) throws MPIException {
        ArrayList<Integer> mess = new ArrayList<>();
        for (Point aHull : hull) {
            mess.add(aHull.x);
            mess.add(aHull.y);
        }
        int[] message = mess.stream().mapToInt(Integer::intValue).toArray();
        MPI.COMM_WORLD.send(message, message.length, MPI.INT, 0, 0);
    }

    static void receiveAndJoinHulls(int size, int numRanks, String outfile, long startTime, ArrayList<Point> pointsa) throws Exception {
        ArrayList<ArrayList<Point>> aggregate = new ArrayList<>();
        // for rank 0
        int myBegin = (int) ((double)0*((double)pointsa.size()/(double)numRanks));
        int myEnd = (int) (myBegin+((double)pointsa.size()/(double) numRanks));
        Point[] store = pointsa.subList(myBegin, myEnd).toArray(new Point[0]);
        List<Point> hull = ConvexHull.convexHull(store, store.length);
        aggregate.add(0, new ArrayList<>(hull));


        // collect results from all ranks

        for (int i = 1; i < numRanks; i++) {
            int[] message = new int[size * 2];
            MPI.COMM_WORLD.recv(message, size*2, MPI.INT, i, 0);
            ArrayList<Point> curr = new ArrayList<>();

            for (int j = 0; j < size*2; j= j+2) {
                if (message[j] == -1) break;
                curr.add(new Point(message[j], message[j + 1]));
            }
            aggregate.add(i, curr);
        }
//         Print Result

        PrintWriter output = new PrintWriter( outfile );
        for (Point temp : joinHulls(aggregate))
            output.println("(" + temp.x + ", " + temp.y + ")");
        output.close( );
        System.out.println("output file: " + outfile + " created.");
        final long endTime = System.currentTimeMillis();
        System.out.println("...\n...\n...\nConvex Hull  points took " + (endTime - startTime) + " msec.");
    }

    // join hulls by computing upper and lower tangents
    static List<Point> joinHulls(ArrayList<ArrayList<Point>> hulls) throws Exception {
        ArrayList<Point> accumulatorLeft = new ArrayList<>();
        ArrayList<Point> currentRight = new ArrayList<>();

        for (int i = 0; i < hulls.size(); i++) {
            if (accumulatorLeft.isEmpty() && i == 0) {
                accumulatorLeft = hulls.get(i);
                continue;
            }
            currentRight = hulls.get(i);
            accumulatorLeft = mergeHulls(accumulatorLeft, currentRight);
        }
        return accumulatorLeft;
    }

    static ArrayList<Point> mergeHulls(ArrayList<Point> left, ArrayList<Point> right) throws Exception {
        left.sort(Comparator.comparingInt(x -> x.x));
        right.sort(Comparator.comparingInt(x -> x.x));

        Point leftMost = left.get(left.size()-1);
        Point rightMost = right.get(0);

        // get top comment tangents
        while(intersects(left, leftMost, rightMost) || intersects(right, leftMost, rightMost)) {
            if (intersects(left, leftMost, rightMost)) for (int i = left.size() - 1; i >= 0; i--)
                if (left.get(i).y > leftMost.y) {
                    leftMost = left.get(i);
                    break;
                }
            if (intersects(right, leftMost, rightMost)) for (int i = 0; i < right.size(); i++)
                if (right.get(i).y > rightMost.y) {
                    rightMost = right.get(i);
                    break;
                }
        }

        Point bleftMost = left.get(left.size()-1);
        Point brightMost = right.get(0);

        // get bottom common tangents
        while(intersects(left, leftMost, brightMost) || intersects(right, leftMost, brightMost)) {
            if (intersects(left, bleftMost, brightMost)) for (int i = left.size() - 1; i >= 0; i--)
                if (left.get(i).y < bleftMost.y) {
                    bleftMost = left.get(i);
                    break;
                }
            if (intersects(right, bleftMost, brightMost)) for (int i = 0; i < right.size(); i++)
                if (right.get(i).y < brightMost.y) {
                    brightMost = right.get(i);
                    break;
                }
        }

        ArrayList<Point> finalHull = new ArrayList<>();
        for (Point point : left) if (point.x < leftMost.x && point.x < bleftMost.x) finalHull.add(point);
        for (Point point : right) if (point.x < rightMost.x && point.x < brightMost.x) finalHull.add(point);

        return finalHull;

    }
    static boolean intersects(ArrayList<Point> points, Point f, Point s) throws Exception {
        final Polygon poly = new Polygon();
        points.forEach(x -> poly.addPoint(x.x, x.y));
        final Line2D.Double line = new Line2D.Double(f.x, f.y, s.x, s.y);
        return getIntersections(poly, line).isEmpty();

    }

    public static Set<Point2D> getIntersections(final Polygon poly, final Line2D.Double line) throws Exception {

        final PathIterator polyIt = poly.getPathIterator(null); //Getting an iterator along the polygon path
        final double[] coords = new double[6]; //Double array with length 6 needed by iterator
        final double[] firstCoords = new double[2]; //First point (needed for closing polygon path)
        final double[] lastCoords = new double[2]; //Previously visited point
        final Set<Point2D> intersections = new HashSet<Point2D>(); //List to hold found intersections
        polyIt.currentSegment(firstCoords); //Getting the first coordinate pair
        lastCoords[0] = firstCoords[0]; //Priming the previous coordinate pair
        lastCoords[1] = firstCoords[1];
        polyIt.next();
        while(!polyIt.isDone()) {
            final int type = polyIt.currentSegment(coords);
            switch(type) {
                case PathIterator.SEG_LINETO : {
                    final Line2D.Double currentLine = new Line2D.Double(lastCoords[0], lastCoords[1], coords[0], coords[1]);
                    if(currentLine.intersectsLine(line))
                        intersections.add(getIntersection(currentLine, line));
                    lastCoords[0] = coords[0];
                    lastCoords[1] = coords[1];
                    break;
                }
                case PathIterator.SEG_CLOSE : {
                    final Line2D.Double currentLine = new Line2D.Double(coords[0], coords[1], firstCoords[0], firstCoords[1]);
                    if(currentLine.intersectsLine(line))
                        intersections.add(getIntersection(currentLine, line));
                    break;
                }
                default : {
                    throw new Exception("Unsupported PathIterator segment type.");
                }
            }
            polyIt.next();
        }
        return intersections;

    }

    public static Point2D getIntersection(final Line2D.Double line1, final Line2D.Double line2) {

        final double x1,y1, x2,y2, x3,y3, x4,y4;
        x1 = line1.x1; y1 = line1.y1; x2 = line1.x2; y2 = line1.y2;
        x3 = line2.x1; y3 = line2.y1; x4 = line2.x2; y4 = line2.y2;
        final double x = (
                (x2 - x1)*(x3*y4 - x4*y3) - (x4 - x3)*(x1*y2 - x2*y1)
        ) /
                (
                        (x1 - x2)*(y3 - y4) - (y1 - y2)*(x3 - x4)
                );
        final double y = (
                (y3 - y4)*(x1*y2 - x2*y1) - (y1 - y2)*(x3*y4 - x4*y3)
        ) /
                (
                        (x1 - x2)*(y3 - y4) - (y1 - y2)*(x3 - x4)
                );

        return new Point2D.Double(x, y);

    }


}

