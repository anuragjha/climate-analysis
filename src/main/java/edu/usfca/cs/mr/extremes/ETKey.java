package edu.usfca.cs.mr.extremes;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ETKey implements Writable {

    private BooleanWritable toSendMinST;
    private BooleanWritable toSendMaxST;
    private BooleanWritable toSendMinAT;
    private BooleanWritable toSendMaxAT;


    public ETKey() {
        this.toSendMinST = new BooleanWritable();
        this.toSendMaxST = new BooleanWritable();
        this.toSendMinAT = new BooleanWritable();
        this.toSendMaxAT = new BooleanWritable();
    }

    public BooleanWritable getToSendMinST() {
        return toSendMinST;
    }

    public ETKey setToSendMinST(BooleanWritable toSendMinST) {
        this.toSendMinST = toSendMinST;
        return this;
    }

    public BooleanWritable getToSendMaxST() {
        return toSendMaxST;
    }

    public ETKey setToSendMaxST(BooleanWritable toSendMaxST) {
        this.toSendMaxST = toSendMaxST;
        return this;
    }

    public BooleanWritable getToSendMinAT() {
        return toSendMinAT;
    }

    public ETKey setToSendMinAT(BooleanWritable toSendMinAT) {
        this.toSendMinAT = toSendMinAT;
        return this;
    }

    public BooleanWritable getToSendMaxAT() {
        return toSendMaxAT;
    }

    public ETKey setToSendMaxAT(BooleanWritable toSendMaxAT) {
        this.toSendMaxAT = toSendMaxAT;
        return this;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        toSendMinST.write(dataOutput);
        toSendMaxST.write(dataOutput);
        toSendMinAT.write(dataOutput);
        toSendMaxAT.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        toSendMinST.readFields(dataInput);
        toSendMaxST.readFields(dataInput);
        toSendMinAT.readFields(dataInput);
        toSendMaxAT.readFields(dataInput);
    }
}
