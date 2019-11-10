package edu.usfca.cs.mr.travelStartup;

public class TotalTempAndHumidity {

    private float airtemp;
    private float humidity;
    private float comfortIndex;
    private int count;

    public float getComfortIndex() {
        return comfortIndex;
    }

    public void setComfortIndex(float comfortIndex) {
        this.comfortIndex = comfortIndex;
    }

    public TotalTempAndHumidity(float airtemp, float humidity, int i){
        this.airtemp = airtemp;
        this.humidity = humidity;
        this.count +=i;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count+= count;
    }

    public float getAirtemp() {
        return airtemp;
    }

    public void setAirtemp(float airtemp) {
        this.airtemp = airtemp;
    }

    public float getHumidity() {
        return humidity;
    }

    public void setHumidity(float humidity) {
        this.humidity = humidity;
    }
}
