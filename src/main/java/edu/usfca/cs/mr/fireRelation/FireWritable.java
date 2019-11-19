package edu.usfca.cs.mr.fireRelation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FireWritable implements Writable {

    private FloatWritable latitude;
    private FloatWritable longitude;
    private Text geohash;
    private Text year;
    private DoubleWritable fireSize;

    public FireWritable() {
        this.latitude = new FloatWritable();
        this.longitude = new FloatWritable();
        this.geohash = new Text();
        this.year = new Text();
        this.fireSize = new DoubleWritable();
    }

    public FloatWritable getLatitude() {
        return latitude;
    }

    public FireWritable setLatitude(FloatWritable latitude) {
        this.latitude = latitude;
        return this;
    }

    public FloatWritable getLongitude() {
        return longitude;
    }

    public FireWritable setLongitude(FloatWritable longitude) {
        this.longitude = longitude;
        return this;
    }

    public Text getGeohash() {
        return geohash;
    }

    public FireWritable setGeohash(Text geohash) {
        this.geohash = geohash;
        return this;
    }

    public Text getYear() {
        return year;
    }

    public FireWritable setYear(Text year) {
        this.year = year;
        return this;
    }

    public DoubleWritable getFireSize() {
        return fireSize;
    }

    public FireWritable setFireSize(DoubleWritable fireSize) {
        this.fireSize = fireSize;
        return this;
    }

    @Override
    public String toString() {
        return "latitude"+latitude+",longitude"+longitude+",geohash"+geohash+",year"+year+",fireSize"+fireSize;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        latitude.write(dataOutput);
        longitude.write(dataOutput);
        geohash.write(dataOutput);
        year.write(dataOutput);
        fireSize.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        latitude.readFields(dataInput);
        longitude.readFields(dataInput);
        geohash.readFields(dataInput);
        year.readFields(dataInput);
        fireSize.readFields(dataInput);
    }
}
