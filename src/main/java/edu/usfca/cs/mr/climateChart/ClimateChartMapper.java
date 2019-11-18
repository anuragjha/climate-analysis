package edu.usfca.cs.mr.climateChart;

import edu.usfca.cs.mr.util.Config;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Line;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ClimateChartMapper
        extends Mapper<LongWritable, Text, Text, CCMapperWritable> {

    Config config = Config.readConfig("config.json");
    String givenGeoHash = config.getClimateChartHash();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        //String givenGeoHash = "9q";
        String geoHash = Geohash.encode(
                Float.parseFloat(Line.getLatitude(line)),
                Float.parseFloat(Line.getLongitude(line)),
                givenGeoHash.length()
        );

        if (givenGeoHash.equalsIgnoreCase(geoHash) && isCleanData(line)) {
            // CCMapperWritable - month, airTemp, precipitation
            //System.out.println("in context write of CCMapper");
            context.write(
                    new Text(Line.getUtc_date(line).substring(4,6)), // month
                    new CCMapperWritable()
                            .setMonthNum(new Text(Line.getUtc_date(line).substring(4,6)))
                            .setGeoHash(new Text(geoHash))
                            .setAirTemp(new DoubleWritable(Line.getAir_temperature(line)))
                            .setPrecip(new DoubleWritable(Line.getPrecipitation(line)))
            );
        }
    }


    private boolean isCleanData(String line) {
        if(Line.getAir_temperature(line) >= 500) {
            return false;
        } else if (Line.getAir_temperature(line) <= -500) {
            return false;
        }
        if(Line.getPrecipitation(line) >= 2000) {
            return false;
        }else if (Line.getPrecipitation(line) < 0) {
            return false;
        }

        return true;
    }


}
