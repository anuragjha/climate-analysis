package edu.usfca.cs.mr.climateChart;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ClimateChartReducer
        extends Reducer<Text, CCMapperWritable, Text, Text> {

    @Override
    protected void reduce(
            Text key, Iterable<CCMapperWritable> values, Context context)
            throws IOException, InterruptedException {

        //System.out.println("in cc reducer");
        double highTemp = -9999;
        double lowTemp = 9999;

        //double avgTemp = 0;
        double sumTemp = 0;

        //double avgPrecipitation = 0;
        double sumPrecip = 0;

        int count = 0;

        String geoHash = "";

        for (CCMapperWritable value : values) {

            geoHash = value.getGeoHash().toString();

            double airTemp = value.getAirTemp().get();
            highTemp = updateHighTemp(highTemp, airTemp);
            lowTemp = updateLowTemp(lowTemp, airTemp);
            sumTemp += airTemp;

            double precip = value.getPrecip().get();
            sumPrecip += precip;

            count += 1;
        }
        double avgTemp = sumTemp / count;
        double avgPrecipitation = sumPrecip / count;


        CCReducerObj obj = new CCReducerObj()
                .setGeoHash(geoHash)
                .setHighTemp(highTemp)
                .setLowTemp(lowTemp)
                .setAvgTemp(avgTemp)
                .setAvgPrecipitation(avgPrecipitation);
        context.write(key, new Text(","+obj.toString()));


    }

    private double updateLowTemp(Double currTemp, Double newTemp) {
        if (currTemp > newTemp) {
            return newTemp;
        }
        return currTemp;
    }

    private double updateHighTemp(Double currTemp, Double newTemp) {
        if (currTemp < newTemp) {
            return newTemp;
        }
        return currTemp;
    }

}
