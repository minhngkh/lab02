package org.khdl06.lab02;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class IntPair implements WritableComparable<IntPair> {
    private int first;
    private int second;

    public IntPair() {
    }

    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    public int compareTo(IntPair intPair) {
        int termIdxComp = Integer.compare(first, intPair.first);
        if (termIdxComp != 0) {
            return termIdxComp;
        }

        return Integer.compare(second, intPair.second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntPair intPair = (IntPair) o;
        return first == intPair.first && second == intPair.second;
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }
}
