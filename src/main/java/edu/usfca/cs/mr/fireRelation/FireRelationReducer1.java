package edu.usfca.cs.mr.fireRelation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class FireRelationReducer1
        extends Reducer<Text, FireWritableFactors, Text, Text> {

    @Override
    protected void reduce(
            Text key, Iterable<FireWritableFactors> values, Context context)
            throws IOException, InterruptedException {

        System.out.println("In Reducer");
        Double sumAT = 0.0;
        Double sumW = 0.0;
        Double sumSR = 0.0;
        Double sumP = 0.0;
        long count = 0;

        for(FireWritableFactors value : values) {
            sumAT += value.getAirTemp().get();
            sumW += value.getWind().get();
            sumSR += value.getSolarRadiation().get();
            sumP += value.getPrecipitation().get();
            count++;
        }
        Double avgAT = sumAT / count;
        Double avgW = sumW / count;
        Double avgSR = sumSR / count;
        Double avgP = sumP / count;

        String str = ","+avgAT+","+avgW+","+avgSR+","+avgP;


        context.write(key, new Text(str)); // from 2006 to 2015
        //context.write(key, new Text("ok"));


    }


}
