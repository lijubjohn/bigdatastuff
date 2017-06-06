import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by liju on 6/5/17.
 */
public class IntPair implements WritableComparable<IntPair> {

    private int first = 0;
    private int second = 0;

    public IntPair() {
        this.first=0;
        this.second=0;
    }

    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public void set(int left, int right) {
        first = left;
        second = right;
    }

    public int compareTo(IntPair o) {
        if (first != o.first) {
            return first < o.first ? -1 : 1;
        } else if (second != o.second) {
            return second < o.second ? -1 : 1;
        } else {
            return 0;
        }
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(first - Integer.MIN_VALUE);
        out.writeInt(second - Integer.MIN_VALUE);
    }

    public void readFields(DataInput in) throws IOException {
        first = in.readInt() + Integer.MIN_VALUE;
        second = in.readInt() + Integer.MIN_VALUE;
    }
}
