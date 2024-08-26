package org.khdl06.lab02;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class DocumentToken implements WritableComparable<DocumentToken> {
    private String value;
    private String fileOrigin;
    private String directoryOrigin;

    public DocumentToken() {
    }

    public DocumentToken(String value, String fileOrigin, String directoryOrigin) {
        this.value = value;
        this.fileOrigin = fileOrigin;
        this.directoryOrigin = directoryOrigin;
    }

    public String getValue() {
        return value;
    }

    public String getFileOrigin() {
        return fileOrigin;
    }

    public String getDirectoryOrigin() {
        return directoryOrigin;
    }

    @Override
    public int compareTo(DocumentToken token) {
        int valueComp = value.compareTo(token.value);
        if (valueComp != 0) {
            return valueComp;
        }

        int fileOriginComp = fileOrigin.compareTo(token.fileOrigin);
        if (fileOriginComp != 0) {
            return fileOriginComp;
        }

        return directoryOrigin.compareTo(token.directoryOrigin);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(value);
        out.writeUTF(fileOrigin);
        out.writeUTF(directoryOrigin);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = in.readUTF();
        fileOrigin = in.readUTF();
        directoryOrigin = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DocumentToken token = (DocumentToken) o;
        return Objects.equals(value, token.value) && Objects.equals(fileOrigin,
                token.fileOrigin) && Objects.equals(directoryOrigin,
                token.directoryOrigin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, fileOrigin, directoryOrigin);
    }

    @Override
    public String toString() {
        return value + "\t" + directoryOrigin + "." + fileOrigin;
    }
}
