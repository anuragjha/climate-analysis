package edu.usfca.cs.mr.FireCorrelation;

import edu.usfca.cs.mr.pcc.RunningStatisticsND;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FireReducer extends Reducer<Text, RunningStatisticsND, Text, Text> {
    private static final String[]  dimensionsArray = new String[]{"FireSize","Air_temperature","Precipitation","Solar", "Wind_1_5"};

    protected void reduce(Text key, Iterable<RunningStatisticsND> values, Context context) throws IOException, InterruptedException {
        System.out.println("In Reducer !!");
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
