package edu.usfca.cs.mr.solarWind;

public class SolarWindAverage  {

    private double solarRadiationAverage;
    private double windSpeedAverage;
    private double minSolarRadiation = 0;
    private double maxSolarRadiation = 0;
    private double minWindSpeed = 0;
    private double maxWindSpeed = 0;

    public SolarWindAverage(double solarRadiationAverage, double windSpeedAverage){
        this.solarRadiationAverage = solarRadiationAverage;
        this.windSpeedAverage = windSpeedAverage;
        if(solarRadiationAverage > maxSolarRadiation ){
            maxSolarRadiation = solarRadiationAverage;
        }
        if(solarRadiationAverage < minSolarRadiation){
            minSolarRadiation = solarRadiationAverage;
        }
        if(windSpeedAverage > maxWindSpeed){
            maxSolarRadiation = windSpeedAverage;
        }
        if(windSpeedAverage < minWindSpeed){
            minSolarRadiation = windSpeedAverage;
        }
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

    public double getMaxSolarRadiation() {
        return maxSolarRadiation;
    }

    public double getMinSolarRadiation() {
        return minSolarRadiation;
    }

    public double getMaxWindSpeed() {
        return maxWindSpeed;
    }

    public double getMinWindSpeed() {
        return minWindSpeed;
    }
}
