package edu.usfca.cs.mr.fireRelation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FireWritableFactors implements Writable {

    private Text geohash;
    private DoubleWritable airTemp;
    private DoubleWritable wind;
    private DoubleWritable solarRadiation;
    private DoubleWritable precipitation;

    public FireWritableFactors() {
        this.geohash = new Text();
        this.airTemp = new DoubleWritable();
        this.wind = new DoubleWritable();
        this.solarRadiation = new DoubleWritable();
        this.precipitation = new DoubleWritable();
    }

    public Text getGeohash() {
        return geohash;
    }

    public FireWritableFactors setGeohash(Text geohash) {
        this.geohash = geohash;
        return this;
    }

    public DoubleWritable getAirTemp() {
        return airTemp;
    }

    public FireWritableFactors setAirTemp(DoubleWritable airTemp) {
        this.airTemp = airTemp;
        return this;
    }

    public DoubleWritable getWind() {
        return wind;
    }

    public FireWritableFactors setWind(DoubleWritable wind) {
        this.wind = wind;
        return this;
    }

    public DoubleWritable getSolarRadiation() {
        return solarRadiation;
    }

    public FireWritableFactors setSolarRadiation(DoubleWritable solarRadiation) {
        this.solarRadiation = solarRadiation;
        return this;
    }

    public DoubleWritable getPrecipitation() {
        return precipitation;
    }

    public FireWritableFactors setPrecipitation(DoubleWritable precipitation) {
        this.precipitation = precipitation;
        return this;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        geohash.write(dataOutput);
        airTemp.write(dataOutput);
        wind.write(dataOutput);
        solarRadiation.write(dataOutput);
        precipitation.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        geohash.readFields(dataInput);
        airTemp.readFields(dataInput);
        wind.readFields(dataInput);
        solarRadiation.readFields(dataInput);
        precipitation.readFields(dataInput);
    }
}
