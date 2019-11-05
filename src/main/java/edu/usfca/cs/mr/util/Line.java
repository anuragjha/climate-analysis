package edu.usfca.cs.mr.util;

import org.apache.hadoop.io.Text;

public class Line {

    public static Text getWbanno(String line) {
        return new Text(line.substring(1, 5));
    }

    public static Text getUtc_date(String line) {
        return new Text(line.substring(7, 14));
    }

    public static Text getUtc_time(String line) {
        return new Text(line.substring(16, 19));
    }

    public static Text getLst_date(String line) {
        return new Text(line.substring(21, 28));
    }

    public static Text getLst_time(String line) {
        return new Text(line.substring(30, 33));
    }

    public static Text getCrx_vn(String line) {
        return new Text(line.substring(35, 40));
    }

    public static Text getLongitude(String line) {
        return new Text(line.substring(42, 48));
    }

    public static Text getLatitude(String line) {
        return new Text(line.substring(50, 56));
    }

    public static Text getAir_temperature(String line) {
        return new Text(line.substring(58, 64));
    }

    public static Text getPrecipitation(String line) {
        return new Text(line.substring(66, 72));
    }

    public static Text getSolar_radiation(String line) {
        return new Text(line.substring(74, 79));
    }

    public static Text getSr_flag(String line) {
        return new Text(line.substring(81, 81));
    }

    public static Text getSurface_temperature(String line) {
        return new Text(line.substring(83, 89));
    }

    public static Text getSt_type(String line) {
        return new Text(line.substring(91, 91));
    }

    public static Text getSt_flag(String line) {
        return new Text(line.substring(93, 93));
    }

    public static Text getRelative_humidity(String line) {
        return new Text(line.substring(95, 99));
    }

    public static Text getRh_flag(String line) {
        return new Text(line.substring(101, 101));
    }

    public static Text getSoil_moisture_5(String line) {
        return new Text(line.substring(103, 109));
    }

    public static Text getSoil_temperature_5(String line) {
        return new Text(line.substring(111, 117));
    }

    public static Text getWetness(String line) {
        return new Text(line.substring(119, 123));
    }

    public static Text getWet_flag(String line) {
        return new Text(line.substring(125, 125));
    }

    public static Text getWind_1_5(String line) {
        return new Text(line.substring(127, 132));
    }

    public static Text getWind_flag(String line) {
        return new Text(line.substring(134, 134));
    }
}
