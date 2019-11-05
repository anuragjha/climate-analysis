package edu.usfca.cs.mr.util;

import org.apache.hadoop.io.Text;

public class Line {

    public static Text getWbanno(String line) {
        return new Text(line.substring(0, 5));
    }

    public static Text getUtc_date(String line) {
        return new Text(line.substring(6, 14));
    }

    public static Text getUtc_time(String line) {
        return new Text(line.substring(15, 19));
    }

    public static Text getLst_date(String line) {
        return new Text(line.substring(20, 28));
    }

    public static Text getLst_time(String line) {
        return new Text(line.substring(29, 33));
    }

    public static Text getCrx_vn(String line) {
        return new Text(line.substring(34, 40));
    }

    public static Text getLongitude(String line) {
        return new Text(line.substring(41, 48));
    }

    public static Text getLatitude(String line) {
        return new Text(line.substring(49, 56));
    }

    public static Text getAir_temperature(String line) {
        return new Text(line.substring(57, 64));
    }

    public static Text getPrecipitation(String line) {
        return new Text(line.substring(65, 72));
    }

    public static Text getSolar_radiation(String line) {
        return new Text(line.substring(73, 79));
    }

    public static Text getSr_flag(String line) {
        return new Text(line.substring(80, 81));
    }

    public static Text getSurface_temperature(String line) {
        return new Text(line.substring(82, 89));
    }

    public static Text getSt_type(String line) {
        return new Text(line.substring(90, 91));
    }

    public static Text getSt_flag(String line) {
        return new Text(line.substring(92, 93));
    }

    public static Text getRelative_humidity(String line) {
        return new Text(line.substring(94, 99));
    }

    public static Text getRh_flag(String line) {
        return new Text(line.substring(100, 101));
    }

    public static Text getSoil_moisture_5(String line) {
        return new Text(line.substring(102, 109));
    }

    public static Text getSoil_temperature_5(String line) {
        return new Text(line.substring(110, 117));
    }

    public static Text getWetness(String line) {
        return new Text(line.substring(118, 123));
    }

    public static Text getWet_flag(String line) {
        return new Text(line.substring(124, 125));
    }

    public static Text getWind_1_5(String line) {
        return new Text(line.substring(126, 132));
    }

    public static Text getWind_flag(String line) {
        return new Text(line.substring(133, 134));
    }
}
