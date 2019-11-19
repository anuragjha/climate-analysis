package edu.usfca.cs.mr.correlation;

import edu.usfca.cs.mr.pcc.RunningStatisticsND;
import org.apache.avro.generic.GenericData.Array;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CorrelationReducer extends Reducer<Text, RunningStatisticsND, Text, Text> {

    private static final String[]  dimensionsArray = new String[]{"solarRadiation","Air_temperature","Precipitation","Relative_humidity", "Wind_1_5", "Soil_temperature_5","Soil_moisture_5","Wetness","Surface_temp"};
    private static final List<String> array = new ArrayList<>(Arrays.asList("solarRadiation","Air_temperature","Precipitation","Relative_humidity", "Wind_1_5", "Soil_temperature_5","Soil_moisture_5","Wetness"));

    @Override
    protected void reduce(Text key, Iterable<RunningStatisticsND> values, Context context) throws IOException, InterruptedException {
        RunningStatisticsND result = new RunningStatisticsND();
        int k = 0;
        for(RunningStatisticsND value : values){
            result.merge(value);
            k+=1;
        }
        System.out.println(" k: "+k);
        int dimensions = result.dimensions();
        int i = 0;
        //double corr = result.r(0,8);
        //System.out.println("Corr : "+corr);
        for (i = 0; i < dimensions ; ++i) {
            for (int j = i + 1; j < dimensions; ++j) {
                    double corr = result.r(i, j);
                    System.out.println("i : "+i+" j : "+j);
                    context.write(new Text(dimensionsArray[i] + "," + dimensionsArray[j]), new Text(String.valueOf(corr)));
            }
        }
    }
}
