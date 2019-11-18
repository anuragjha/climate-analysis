package edu.usfca.cs.mr.travelStartup;

public class TotalTempAndHumidity {

    private float airtemp;
    private float humidity;
    private float comfortIndex;
    private int count;
    private String latitude;

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        if(this.latitude == "") {
            this.latitude = latitude;
        }
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        if(this.longitude == "") {
            this.longitude = longitude;
        }
    }

    private String longitude;

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
        this.latitude = "";
        this.longitude = "";
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

    public float getAirTemperatureinFahreneit(){
        float airtempInFahreneit = (airtemp * 9/5) + 32;
        return  airtempInFahreneit;
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
