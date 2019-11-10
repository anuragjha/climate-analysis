package edu.usfca.cs.mr.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Line {

    public static String getWbanno(String line) {
        return (line.substring(0, 5));
    }

    public static String getUtc_date(String line) {
        return (line.substring(6, 14));
    }

    public static String getUtc_time(String line) {
        return (line.substring(15, 19));
    }

    public static String getLst_date(String line) {
        return (line.substring(20, 28));
    }

    public static String getLst_time(String line) {
        return (line.substring(29, 33));
    }

    public static String getCrx_vn(String line) {
        return (line.substring(34, 40));
    }

    public static String getLongitude(String line) {
        return (line.substring(41, 48));
    }

    public static String getLatitude(String line) {
        return (line.substring(49, 56));
    }

    public static Double getAir_temperature(String line) {
        return Double.parseDouble(line.substring(57, 64));
    }

    public static Double getPrecipitation(String line) {
        return Double.parseDouble(line.substring(65, 72));
    }

    public static Double getSolar_radiation(String line) {
        return Double.parseDouble(line.substring(73, 79));
    }

    public static String getSr_flag(String line) {
        return (line.substring(80, 81));
    }

    public static Double getSurface_temperature(String line) {
        return Double.parseDouble(line.substring(82, 89));
    }

    public static String getSt_type(String line) {
        return (line.substring(90, 91));
    }

    public static String getSt_flag(String line) {
        return (line.substring(92, 93));
    }

    public static Double getRelative_humidity(String line) {
        return Double.parseDouble(line.substring(94, 99));
    }

    public static String getRh_flag(String line) {
        return (line.substring(100, 101));
    }

    public static Double getSoil_moisture_5(String line) {
        return Double.parseDouble(line.substring(102, 109));
    }

    public static Double getSoil_temperature_5(String line) {
        return Double.parseDouble(line.substring(110, 117));
    }

    public static Double getWetness(String line) {
        return Double.parseDouble(line.substring(118, 123));
    }

    public static String getWet_flag(String line) {
        return (line.substring(124, 125));
    }

    public static Double getWind_1_5(String line) {
        return Double.parseDouble(line.substring(126, 132));
    }

    public static String getWind_flag(String line) {
        return (line.substring(133, 134));
    }
}
