package edu.usfca.cs.mr.solarWind;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SolarWindDetails implements Writable {

    private DoubleWritable solarRadiation;
    private DoubleWritable windSpeed;
    private FloatWritable latitude;
    private FloatWritable longitude;


    public SolarWindDetails(){
        this.solarRadiation = new DoubleWritable();
        this.windSpeed = new DoubleWritable();
        this.latitude = new FloatWritable();
        this.longitude = new FloatWritable();
    }

    public SolarWindDetails(double solarRadiation, double windSpeed, float latitude, float longitude){
        this.solarRadiation = new DoubleWritable(solarRadiation);
        this.windSpeed = new DoubleWritable(windSpeed);
        this.latitude = new FloatWritable(latitude);
        this.longitude = new FloatWritable(longitude);
    }

    public void set(DoubleWritable solarRadiation, DoubleWritable windSpeed, FloatWritable latitude, FloatWritable longitude){
        this.solarRadiation = solarRadiation;
        this.windSpeed = windSpeed;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public double getSolarRadiation() {
        return solarRadiation.get();
    }

    public void setSolarRadiation(double solarRadiation) {
        this.solarRadiation = new DoubleWritable(solarRadiation);
    }

    public double getWindSpeed() {
        return windSpeed.get();
    }

    public void setWindSpeed(double windSpeed) {
        this.windSpeed = new DoubleWritable(windSpeed);
    }

    public float getLatitude() {
        return latitude.get();
    }

    public void setLatitude(float latitude) {
        this.latitude = new FloatWritable(latitude);
    }

    public float getLongitude() {
        return longitude.get();
    }

    public void setLongitude(float longitude) {
        this.longitude = new FloatWritable(longitude);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        solarRadiation.write(dataOutput);
        windSpeed.write(dataOutput);
        latitude.write(dataOutput);
        longitude.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        solarRadiation.readFields(dataInput);
        windSpeed.readFields(dataInput);
        latitude.readFields(dataInput);
        longitude.readFields(dataInput);
    }
}
