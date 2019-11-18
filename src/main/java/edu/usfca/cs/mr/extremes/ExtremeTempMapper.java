package edu.usfca.cs.mr.extremes;

import edu.usfca.cs.mr.util.Line;
import edu.usfca.cs.mr.util.NCDCWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ExtremeTempMapper
extends Mapper<LongWritable, Text, Text, ETWritable> {

    private double minSurfaceTemp = 9999;
    private double maxSurfaceTemp = -9999;
    private double minAirTemp = 9999;
    private double maxAirTemp = -9999;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // emit month-day, ETWritable.
        String line = value.toString();

        Double currAirTemp = Line.getAir_temperature(line);
        Double currSurfaceTemp = Line.getSurface_temperature(line);

        boolean toSendMinST = false;
        boolean toSendMaxST = false;
        boolean toSendMinAT = false;
        boolean toSendMaxAT = false;

        if(isCleanData(line)) {
            if(minSurfaceTemp > currSurfaceTemp) {
                toSendMinST = true;
                minSurfaceTemp = currSurfaceTemp;
            }
            if(maxSurfaceTemp < currSurfaceTemp) {
                toSendMaxST = true;
                maxSurfaceTemp = currSurfaceTemp;
            }
            if(minAirTemp > currAirTemp) {
                toSendMinAT = true;
                minAirTemp = currAirTemp;
            }
            if(maxAirTemp < currAirTemp) {
                toSendMaxAT = true;
                maxAirTemp = currAirTemp;
            }
        }

        if(toSendMinST || toSendMaxST || toSendMinAT || toSendMaxAT) {
//            ETempWritable etw = new ETempWritable()
//                    .setSurFaceTemp(new DoubleWritable(currSurfaceTemp))
//                    .setAirTemp(new DoubleWritable(currAirTemp));

//            ETKey etk = new ETKey()
//                    .setToSendMinST(new BooleanWritable(toSendMinST))
//                    .setToSendMaxST(new BooleanWritable(toSendMaxST))
//                    .setToSendMinAT(new BooleanWritable(toSendMinAT))
//                    .setToSendMaxST(new BooleanWritable(toSendMaxAT));

            ETWritable etw = new ETWritable()
                    .setLatitude(new Text(Line.getLatitude(line)))
                    .setLongitude(new Text(Line.getLongitude(line)))
                    .setUtcDate(new Text(Line.getUtc_date(line)))
                    .setUtcTime(new Text(Line.getUtc_time(line)))
                    .setSurFaceTemp(new DoubleWritable(currSurfaceTemp))
                    .setAirTemp(new DoubleWritable(currAirTemp));

//            System.out.println(etw.toString());
            context.write(
//                    etk,
                    new Text(Line.getUtc_date(line).substring(0,4)),
                    etw
            );


        }


    }


    private boolean isCleanData(String line) {
        if(Line.getAir_temperature(line) >= 500) {
            return false;
        } else if (Line.getAir_temperature(line) <= -500) {
            return false;
        }
        if(Integer.parseInt(Line.getSt_flag(line)) != 0) {
            return false;
        }
        if(Line.getSurface_temperature(line) >= 500) {
            return false;
        }else if (Line.getSurface_temperature(line) <= -500) {
            return false;
        }

        return true;
    }



}
