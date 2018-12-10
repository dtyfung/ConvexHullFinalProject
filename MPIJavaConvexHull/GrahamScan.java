import java.util.Arrays;
import java.util.Stack;
import java.util.stream.IntStream;

class GrahamScan {
    private Stack<Point2D> hull = new Stack<Point2D>();

    GrahamScan(Point2D[] points) {
        // defensive copy
        int n = points.length;
        Point2D[] a = new Point2D[n];
        System.arraycopy(points, 0, a, 0, n);

        Arrays.sort(a); // preprocess so that a[0] has lowest y-coordinate

        Arrays.sort(a, 1, n, a[0].polarOrder()); // sort by polar angle with respect to base point a[0], breaking ties by distance to a[0]

        hull.push(a[0]);       // a[0] is first extreme point

        // find index k1 of first point not equal to a[0]
        int k1;
        for (k1 = 1; k1 < n; k1++)
            if (!a[0].equals(a[k1])) break;
        if (k1 == n) return;        // all points equal

        // find index k2 of first point not collinear with a[0] and a[k1]
        int k2;
        for (k2 = k1+1; k2 < n; k2++)
            if (Point2D.ccw(a[0], a[k1], a[k2]) != 0) break;
        hull.push(a[k2-1]);    // a[k2-1] is second extreme point

        // Graham scan; note that a[n-1] is extreme point different from a[0]
        for (int i = k2; i < n; i++) {
            Point2D top = hull.pop();
            while (Point2D.ccw(hull.peek(), top, a[i]) <= 0) top = hull.pop();
            hull.push(top);
            hull.push(a[i]);
        }

        assert isConvex();
    }


    public Iterable<Point2D> hull() {
        Stack<Point2D> s = new Stack<Point2D>();
        hull.forEach(s::push);
        return s;
    }


    private boolean isConvex() {
        int n = hull.size();
        if (n <= 2) return true;

        Point2D[] points = new Point2D[n];
        int k = 0;
        for (Point2D p : hull()) points[k++] = p;
        return IntStream.range(0, n).noneMatch(i -> Point2D.ccw(points[i], points[(i + 1) % n], points[(i + 2) % n]) <= 0);
    }

}
