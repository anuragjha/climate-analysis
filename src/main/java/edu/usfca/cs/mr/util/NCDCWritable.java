package edu.usfca.cs.mr.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NCDCWritable implements Writable {
//Field#  Name                           Units
//---------------------------------------------
//   1    WBANNO                         XXXXX
//   2    UTC_DATE                       YYYYMMDD
//   3    UTC_TIME                       HHmm
//   4    LST_DATE                       YYYYMMDD
//   5    LST_TIME                       HHmm
//   6    CRX_VN                         XXXXXX
//   7    LONGITUDE                      Decimal_degrees
//   8    LATITUDE                       Decimal_degrees
//   9    AIR_TEMPERATURE                Celsius
//   10   PRECIPITATION                  mm
//   11   SOLAR_RADIATION                W/m^2
//   12   SR_FLAG                        X
//   13   SURFACE_TEMPERATURE            Celsius
//   14   ST_TYPE                        X
//   15   ST_FLAG                        X
//   16   RELATIVE_HUMIDITY              %
//   17   RH_FLAG                        X
//   18   SOIL_MOISTURE_5                m^3/m^3
//   19   SOIL_TEMPERATURE_5             Celsius
//   20   WETNESS                        Ohms
//   21   WET_FLAG                       X
//   22   WIND_1_5                       m/s
//   23   WIND_FLAG                      X

    private Text wbanno;
    private Text utc_date;
    private Text utc_time;
    private Text lst_date;
    private Text lst_time;
    private Text crx_vn;
    private Text longitude;
    private Text latitude;
    private Text air_temperature;
    private Text precipitation;
    private Text solar_radiation;
    private Text sr_flag;
    private Text surface_temperature;
    private Text st_type;
    private Text st_flag;
    private Text relative_humidity;
    private Text rh_flag;
    private Text soil_moisture_5;
    private Text soil_temperature_5;
    private Text wetness;
    private Text wet_flag;
    private Text wind_1_5;
    private Text wind_flag;
    private Text geohash;


    public NCDCWritable() {
        super();
        this.wbanno = new Text();
        this.utc_date = new Text();
        this.utc_time = new Text();
        this.lst_date = new Text();
        this.lst_time = new Text();
        this.crx_vn = new Text();
        this.longitude = new Text();
        this.latitude = new Text();
        this.air_temperature = new Text();
        this.precipitation = new Text();
        this.solar_radiation = new Text();
        this.sr_flag = new Text();
        this.surface_temperature = new Text();
        this.st_type = new Text();
        this.st_flag = new Text();
        this.relative_humidity = new Text();
        this.rh_flag = new Text();
        this.soil_moisture_5 = new Text();
        this.soil_temperature_5 = new Text();
        this.wetness = new Text();
        this.wet_flag = new Text();
        this.wind_1_5 = new Text();
        this.wind_flag = new Text();
        this.geohash = new Text();
    }

    public Text getGeohash() {
        return geohash;
    }

    public NCDCWritable setGeohash(Text geohash) {
        this.geohash = geohash;
        return this;
    }

    public Text getWbanno() {
        return wbanno;
    }

    public NCDCWritable setWbanno(Text wbanno) {
        this.wbanno = wbanno;
        return this;
    }

    public Text getUtc_date() {
        return utc_date;
    }

    public NCDCWritable setUtc_date(Text utc_date) {
        this.utc_date = utc_date;
        return this;
    }

    public Text getUtc_time() {
        return utc_time;
    }

    public NCDCWritable setUtc_time(Text utc_time) {
        this.utc_time = utc_time;
        return this;
    }

    public Text getLst_date() {
        return lst_date;
    }

    public NCDCWritable setLst_date(Text lst_date) {
        this.lst_date = lst_date;
        return this;
    }

    public Text getLst_time() {
        return lst_time;
    }

    public NCDCWritable setLst_time(Text lst_time) {
        this.lst_time = lst_time;
        return this;
    }

    public Text getCrx_vn() {
        return crx_vn;
    }

    public NCDCWritable setCrx_vn(Text crx_vn) {
        this.crx_vn = crx_vn;
        return this;
    }

    public Text getLongitude() {
        return longitude;
    }

    public NCDCWritable setLongitude(Text longitude) {
        this.longitude = longitude;
        return this;
    }

    public Text getLatitude() {
        return latitude;
    }

    public NCDCWritable setLatitude(Text latitude) {
        this.latitude = latitude;
        return this;
    }

    public Text getAir_temperature() {
        return air_temperature;
    }

    public NCDCWritable setAir_temperature(Text air_temperature) {
        this.air_temperature = air_temperature;
        return this;
    }

    public Text getPrecipitation() {
        return precipitation;
    }

    public NCDCWritable setPrecipitation(Text precipitation) {
        this.precipitation = precipitation;
        return this;
    }

    public Text getSolar_radiation() {
        return solar_radiation;
    }

    public NCDCWritable setSolar_radiation(Text solar_radiation) {
        this.solar_radiation = solar_radiation;
        return this;
    }

    public Text getSr_flag() {
        return sr_flag;
    }

    public NCDCWritable setSr_flag(Text sr_flag) {
        this.sr_flag = sr_flag;
        return this;
    }

    public Text getSurface_temperature() {
        return surface_temperature;
    }

    public NCDCWritable setSurface_temperature(Text surface_temperature) {
        this.surface_temperature = surface_temperature;
        return this;
    }

    public Text getSt_type() {
        return st_type;
    }

    public NCDCWritable setSt_type(Text st_type) {
        this.st_type = st_type;
        return this;
    }

    public Text getSt_flag() {
        return st_flag;
    }

    public NCDCWritable setSt_flag(Text st_flag) {
        this.st_flag = st_flag;
        return this;
    }

    public Text getRelative_humidity() {
        return relative_humidity;
    }

    public NCDCWritable setRelative_humidity(Text relative_humidity) {
        this.relative_humidity = relative_humidity;
        return this;
    }

    public Text getRh_flag() {
        return rh_flag;
    }

    public NCDCWritable setRh_flag(Text rh_flag) {
        this.rh_flag = rh_flag;
        return this;
    }

    public Text getSoil_moisture_5() {
        return soil_moisture_5;
    }

    public NCDCWritable setSoil_moisture_5(Text soil_moisture_5) {
        this.soil_moisture_5 = soil_moisture_5;
        return this;
    }

    public Text getSoil_temperature_5() {
        return soil_temperature_5;
    }

    public NCDCWritable setSoil_temperature_5(Text soil_temperature_5) {
        this.soil_temperature_5 = soil_temperature_5;
        return this;
    }

    public Text getWetness() {
        return wetness;
    }

    public NCDCWritable setWetness(Text wetness) {
        this.wetness = wetness;
        return this;
    }

    public Text getWet_flag() {
        return wet_flag;
    }

    public NCDCWritable setWet_flag(Text wet_flag) {
        this.wet_flag = wet_flag;
        return this;
    }

    public Text getWind_1_5() {
        return wind_1_5;
    }

    public NCDCWritable setWind_1_5(Text wind_1_5) {
        this.wind_1_5 = wind_1_5;
        return this;
    }

    public Text getWind_flag() {
        return wind_flag;
    }

    public NCDCWritable setWind_flag(Text wind_flag) {
        this.wind_flag = wind_flag;
        return this;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        wbanno.readFields(dataInput);
        utc_date.readFields(dataInput);
        utc_time.readFields(dataInput);
        lst_date.readFields(dataInput);
        lst_time.readFields(dataInput);
        crx_vn.readFields(dataInput);
        longitude.readFields(dataInput);
        latitude.readFields(dataInput);
        air_temperature.readFields(dataInput);
        precipitation.readFields(dataInput);
        solar_radiation.readFields(dataInput);
        sr_flag.readFields(dataInput);
        surface_temperature.readFields(dataInput);
        st_type.readFields(dataInput);
        st_flag.readFields(dataInput);
        relative_humidity.readFields(dataInput);
        rh_flag.readFields(dataInput);
        soil_moisture_5.readFields(dataInput);
        soil_temperature_5.readFields(dataInput);
        wetness.readFields(dataInput);
        wet_flag.readFields(dataInput);
        wind_1_5.readFields(dataInput);
        wind_flag.readFields(dataInput);
        geohash.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        wbanno.write(dataOutput);
        utc_date.write(dataOutput);
        utc_time.write(dataOutput);
        lst_date.write(dataOutput);
        lst_time.write(dataOutput);
        crx_vn.write(dataOutput);
        longitude.write(dataOutput);
        latitude.write(dataOutput);
        air_temperature.write(dataOutput);
        precipitation.write(dataOutput);
        solar_radiation.write(dataOutput);
        sr_flag.write(dataOutput);
        surface_temperature.write(dataOutput);
        st_type.write(dataOutput);
        st_flag.write(dataOutput);
        relative_humidity.write(dataOutput);
        rh_flag.write(dataOutput);
        soil_moisture_5.write(dataOutput);
        soil_temperature_5.write(dataOutput);
        wetness.write(dataOutput);
        wet_flag.write(dataOutput);
        wind_1_5.write(dataOutput);
        wind_flag.write(dataOutput);
        geohash.write(dataOutput);
    }

    public String toString() {
        return utc_date.toString() + " "+ utc_time.toString() + " "+ longitude.toString()+ " " + latitude.toString();
    }

}
