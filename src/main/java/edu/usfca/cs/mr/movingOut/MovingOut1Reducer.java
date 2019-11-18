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
        String latitude = "";
        String longitude = "";
        double sumAT = 0;
        double sumP = 0;
//        double sumSR = 0;
        double sumW = 0;
        int count = 0;

        //System.out.println("GeoHash: "+ key);
        for (NCDCWritable val : values) {

            if (!latitude.equalsIgnoreCase(val.getLatitude().toString())) {
                latitude = val.getLatitude().toString();
                //System.out.println("Latitude: "+ latitude);
            }
            if (!longitude.equalsIgnoreCase(val.getLongitude().toString())) {
                longitude = val.getLongitude().toString();
                //System.out.println("Longitude: "+ longitude);
            }

            sumAT += val.getAir_temperature().get();
            sumP += val.getPrecipitation().get();
//            sumSR += val.getSolar_radiation().get();
            sumW += val.getWind_1_5().get();
            count++;
        }

        double avgAT = sumAT / count;
        double avgP = sumP / count;
//        double avgSR = sumSR / count;
        double avgW = sumW / count;


        String outStr = ","+latitude+","+longitude+","+avgAT+","+avgP+/*","+avgSR+*/","+avgW;
        //System.out.println(outStr);
        context.write(key, new Text(outStr));
    }


}
