package edu.usfca.cs.mr.solarWind;

public class SolarWindAverage  {

    private double solarRadiationAverage;
    private double windSpeedAverage;

    public SolarWindAverage(double solarRadiationAverage, double windSpeedAverage){
        this.solarRadiationAverage = solarRadiationAverage;
        this.windSpeedAverage = windSpeedAverage;
    }

    public double getSolarRadiationAverage() {
        return solarRadiationAverage;
    }

    public void setSolarRadiationAverage(double solarRadiationAverage) {
        this.solarRadiationAverage = solarRadiationAverage;
    }

    public double getWindSpeedAverage() {
        return windSpeedAverage;
    }

    public void setWindSpeedAverage(double windSpeedAverage) {
        this.windSpeedAverage = windSpeedAverage;
    }
}
