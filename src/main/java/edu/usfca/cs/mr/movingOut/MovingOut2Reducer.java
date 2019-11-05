package edu.usfca.cs.mr.movingOut;

import edu.usfca.cs.mr.util.NCDCWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovingOut2Reducer extends Reducer<Text, NCDCWritable, Text, Text> {


    //    9q4	avgAT: 14.825001, avgP: 2.5, avgSR: 95.5
    float reqAT = (float) 14.825001;
    float reqP = (float) 2.5;
    float reqSR = (float) 95.5;
    float deviation = (float)0.1; // % of change from original req values

    @Override
    protected void reduce(Text key, Iterable<NCDCWritable> values, Context context)
            throws IOException, InterruptedException {

        System.out.println("In Reducer");
        float sumAT = 0;
        float sumP = 0;
        float sumSR = 0;
        int count = 0;

        for (NCDCWritable val : values) {

            sumAT += Float.parseFloat(val.getAir_temperature().toString());
            sumP += Float.parseFloat(val.getPrecipitation().toString());
            sumSR += Float.parseFloat(val.getSolar_radiation().toString());
            count++;
        }

        float avgAT = sumAT / count;
        float avgP = sumP / count;
        float avgSR = sumSR / count;


        String outStr = "avgAT: "+ avgAT+ ", avgP: "+ avgP+ ", avgSR: "+avgSR;
        System.out.println(outStr);
        if((avgAT > reqAT-reqAT*deviation && avgAT < reqAT+reqAT*deviation)
                && (avgP > reqP-reqP*deviation && avgP < reqP+reqP*deviation)
                && (avgSR > reqSR-reqSR*deviation && avgP < reqSR+reqSR*deviation)){ // if all conditions in if verified
            context.write(key, new Text(outStr));
        }

    }


}
