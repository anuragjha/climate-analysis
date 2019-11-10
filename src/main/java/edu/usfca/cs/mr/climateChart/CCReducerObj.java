package edu.usfca.cs.mr.climateChart;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CCReducerObj {


    private double highTemp;
    private double lowTemp;
    private double avgTemp;
    private double avgPrecipitation;

    public CCReducerObj() {
        this.highTemp = 0;
        this.lowTemp = 0;
        this.avgTemp = 0;
        this.avgPrecipitation = 0;
    }


    public double getHighTemp() {
        return highTemp;
    }

    public CCReducerObj setHighTemp(double highTemp) {
        this.highTemp = highTemp;
        return this;
    }

    public double getLowTemp() {
        return lowTemp;
    }

    public CCReducerObj setLowTemp(double lowTemp) {
        this.lowTemp = lowTemp;
        return this;
    }

    public double getAvgTemp() {
        return avgTemp;
    }

    public CCReducerObj setAvgTemp(double avgTemp) {
        this.avgTemp = avgTemp;
        return this;
    }

    public double getAvgPrecipitation() {
        return avgPrecipitation;
    }

    public CCReducerObj setAvgPrecipitation(double avgPrecipitation) {
        this.avgPrecipitation = avgPrecipitation;
        return this;
    }

    @Override
    public String toString() {
        //<month-num>  <high-temp>  <low-temp>  <avg-precip>  <avg-temp>
        return " " + highTemp + " " + lowTemp + " " + avgPrecipitation + " " + avgTemp;
    }

}
