package edu.usfca.cs.mr.climateChart;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CCMapperWritable implements Writable {

    private Text monthNum;
    private DoubleWritable airTemp;
    private DoubleWritable precip;

    public CCMapperWritable() {
        monthNum = new Text();
        airTemp = new DoubleWritable();
        precip = new DoubleWritable();
    }

    public Text getMonthNum() {
        return monthNum;
    }

    public CCMapperWritable setMonthNum(Text monthNum) {
        this.monthNum = monthNum;
        return this;
    }

    public DoubleWritable getAirTemp() {
        return airTemp;
    }

    public CCMapperWritable setAirTemp(DoubleWritable airTemp) {
        this.airTemp = airTemp;
        return this;
    }

    public DoubleWritable getPrecip() {
        return precip;
    }

    public CCMapperWritable setPrecip(DoubleWritable precip) {
        this.precip = precip;
        return this;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        monthNum.write(dataOutput);
        airTemp.write(dataOutput);
        precip.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        monthNum.readFields(dataInput);
        airTemp.readFields(dataInput);
        precip.readFields(dataInput);
    }
}
