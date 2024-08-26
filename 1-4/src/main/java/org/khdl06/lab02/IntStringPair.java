package org.khdl06.lab02;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class IntStringPair implements WritableComparable<IntStringPair> {
    private int first;
    private String second;

    public IntStringPair(int first, String second) {
        this.first = first;
        this.second = second;
    }

    public int getInt() {
        return first;
    }

    public String getStr() {
        return second;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }


    @Override
    public int compareTo(IntStringPair intStringPair) {
        int comp = Integer.compare(first, intStringPair.first);
        if (comp != 0) {
            return comp;
        }

        return second.compareTo(intStringPair.second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeUTF(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntStringPair that = (IntStringPair) o;
        return first == that.first && Objects.equals(second, that.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }
}
