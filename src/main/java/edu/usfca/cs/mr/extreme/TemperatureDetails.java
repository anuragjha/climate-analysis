package edu.usfca.cs.mr.extreme;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TemperatureDetails implements Writable {

    private FloatWritable airTemperature;
    private FloatWritable surfaceTemperature;
    private FloatWritable latitude;
    private FloatWritable longitude;
    private LongWritable date;
    private LongWritable time;

    public TemperatureDetails()
    {
        this.airTemperature = new FloatWritable();
        this.surfaceTemperature = new FloatWritable();
        this.latitude = new FloatWritable();
        this.longitude = new FloatWritable();
        this.date = new LongWritable();
        this.time = new LongWritable();
    }

    public TemperatureDetails(float airTemperature,float surfaceTemperature,float latitude,float longitude,long date,long time){
        this.airTemperature = new FloatWritable(airTemperature);
        this.surfaceTemperature =new FloatWritable( surfaceTemperature);
        this.latitude = new FloatWritable(latitude);
        this.longitude = new FloatWritable(longitude);
        this.date = new LongWritable(date);
        this.time = new LongWritable(time);
    }

    public void set(FloatWritable airTemperature, FloatWritable surfaceTemperature, FloatWritable latitude, FloatWritable longitude, LongWritable date,LongWritable time)
        {
            this.airTemperature = airTemperature;
            this.surfaceTemperature = surfaceTemperature;
            this.latitude = latitude;
            this.longitude = longitude;
            this.date = date;
            this.time = time;
        }

    public float getAirTemperature() {
        return airTemperature.get();
    }

    public void setAirTemperature(float airTemperature) {
        this.airTemperature = new FloatWritable(airTemperature);
    }

    public float getSurfaceTemperature() {
        return surfaceTemperature.get();
    }

    public void setSurfaceTemperature(float surfaceTemperature) {
        this.surfaceTemperature = new FloatWritable(surfaceTemperature);
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

    public long getDate() {
        return date.get();
    }

    public void setDate(long date) {
        this.date = new LongWritable(date);
    }

    public long getTime() {
        return time.get();
    }

    public void setTime(long time) {
        this.time = new LongWritable(time);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
       airTemperature.write(dataOutput);
       surfaceTemperature.write(dataOutput);
       latitude.write(dataOutput);
       longitude.write(dataOutput);
       date.write(dataOutput);
       time.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airTemperature.readFields(dataInput);
        surfaceTemperature.readFields(dataInput);
        latitude.readFields(dataInput);
        longitude.readFields(dataInput);
        date.readFields(dataInput);
        time.readFields(dataInput);
    }

    @Override
    public String toString(){
        return String.format(" Airtemp : "+this.airTemperature+" SurfaceTemp : "+this.surfaceTemperature+" Latitude : "+this.latitude+" Longitude : "+this.longitude);
    }
}
