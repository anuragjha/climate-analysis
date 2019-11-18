package edu.usfca.cs.mr.extremes;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ETWritable implements Writable {

    private Text latitude;
    private Text longitude;
    private Text utcDate;
    private Text utcTime;
    private DoubleWritable airTemp;
    private DoubleWritable surfaceTemp;

    public ETWritable() {
        this.latitude = new Text();
        this.longitude = new Text();
        this.utcDate = new Text();
        this.utcTime = new Text();
        this.airTemp = new DoubleWritable();
        this.surfaceTemp = new DoubleWritable();
    }

    public Text getLatitude() {
        return latitude;
    }

    public ETWritable setLatitude(Text latitude) {
        this.latitude = latitude;
        return this;
    }

    public Text getLongitude() {
        return longitude;
    }

    public ETWritable setLongitude(Text longitude) {
        this.longitude = longitude;
        return this;
    }

    public Text getUtcDate() {
        return utcDate;
    }

    public ETWritable setUtcDate(Text utcData) {
        this.utcDate = utcData;
        return this;
    }

    public Text getUtcTime() {
        return utcTime;
    }

    public ETWritable setUtcTime(Text utcTime) {
        this.utcTime = utcTime;
        return this;
    }

    public DoubleWritable getAirTemp() {
        return airTemp;
    }

    public ETWritable setAirTemp(DoubleWritable airTemp) {
        this.airTemp = airTemp;
        return this;
    }

    public DoubleWritable getSurFaceTemp() {
        return surfaceTemp;
    }

    public ETWritable setSurFaceTemp(DoubleWritable surFaceTemp) {
        this.surfaceTemp = surFaceTemp;
        return this;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        latitude.write(dataOutput);
        longitude.write(dataOutput);
        utcDate.write(dataOutput);
        utcTime.write(dataOutput);
        airTemp.write(dataOutput);
        surfaceTemp.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        latitude.readFields(dataInput);
        longitude.readFields(dataInput);
        utcDate.readFields(dataInput);
        utcTime.readFields(dataInput);
        airTemp.readFields(dataInput);
        surfaceTemp.readFields(dataInput);
    }

    @Override
    public String toString() {
        return utcDate + ","+ utcTime + ","+ latitude.toString()+ ","+ longitude.toString()+ ","+ airTemp.toString() + ","+ surfaceTemp.toString()+"\n";
    }
}
