package edu.usfca.cs.mr.correlation;

import edu.usfca.cs.mr.pcc.RunningStatisticsND;
import org.apache.avro.generic.GenericData.Array;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class CorrelationReducer extends Reducer<Text, RunningStatisticsND, Text, Text> {

    private static final String[]  dimensionsArray = new String[]{"solarRadiation","Air_temperature","Precipitation","Relative_humidity", "Wind_1_5", "Soil_temperature_5","Soil_moisture_5","Wetness"};
    @Override
    protected void reduce(Text key, Iterable<RunningStatisticsND> values, Context context) throws IOException, InterruptedException {
        RunningStatisticsND result = new RunningStatisticsND();

        System.out.println("In Reducer !!");
        int k = 0;
        for(RunningStatisticsND value : values){
            result.merge(value);
            k+=1;
        }
        System.out.println(" k: "+k);
        int dimensions = result.dimensions();
        int i = 1;
        while (i <= dimensions){
            for(int j = 1; j <= dimensions; j++){
                double corr = result.r(i,j);
                context.write(new Text(" i : "+i+" j : "+j),new Text(String.valueOf(corr)));
                //context.write(new Text(dimensionsArray[i-1]+" , "+dimensionsArray[j-1]),new Text(String.valueOf(corr)));
            }
            i++;
        }
    }
}
