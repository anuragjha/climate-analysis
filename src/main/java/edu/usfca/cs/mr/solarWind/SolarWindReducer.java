package edu.usfca.cs.mr.solarWind;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;

public class SolarWindReducer extends Reducer<Text,SolarWindDetails, Text,Text> {

    private HashMap<String,SolarWindAverage> geoHashToAverageSolarRadiation = new HashMap<String, SolarWindAverage>();

    protected void reduce(Text key, Iterable<SolarWindDetails> values, Context context)
            throws IOException, InterruptedException{

        System.out.println("In Reducer!!");

        double totalSolar = 0.0;
        double totalWindSpeed = 0.0;
        int count = 0;
        System.out.println("GeoHash : "+key);
        for(SolarWindDetails details : values){
            count += 1;
            totalSolar += details.getSolarRadiation();
            totalWindSpeed += details.getWindSpeed();
        }

        SolarWindAverage average = new SolarWindAverage(totalSolar/count,totalWindSpeed/count);

        geoHashToAverageSolarRadiation.put(key.toString(),average);

        context.write(key,new Text(average.getSolarRadiationAverage()+","+average.getWindSpeedAverage()));
    }
}
