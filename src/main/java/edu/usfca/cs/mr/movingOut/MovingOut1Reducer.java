package edu.usfca.cs.mr.movingOut;

import edu.usfca.cs.mr.util.NCDCWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovingOut1Reducer extends Reducer<Text, NCDCWritable, Text, Text> {

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
        context.write(key, new Text(outStr));
    }


}
