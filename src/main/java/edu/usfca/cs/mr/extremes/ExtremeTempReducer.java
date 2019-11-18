package edu.usfca.cs.mr.extremes;

import edu.usfca.cs.mr.util.Line;
import edu.usfca.cs.mr.util.NCDCWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExtremeTempReducer
        extends Reducer<Text, ETWritable, Text, Text> {


    @Override
    protected void reduce(
            Text key, Iterable<ETWritable> values, Context context)
            throws IOException, InterruptedException {

//        System.out.println("In reducer");

        double minSurfaceTemp = 9999;
        double maxSurfaceTemp = -9999;
        double minAirTemp = 9999;
        double maxAirTemp = -9999;

        List<String> toWrite = new ArrayList<>();
         String minSurfaceTempETW = "";
         String maxSurfaceTempETW = "";
         String minAirTempETW = "";
         String maxAirTempETW = "";


        for(ETWritable value : values) {
//            System.out.println(value.toString());

            Double currAirTemp = value.getAirTemp().get();
            Double currSurfaceTemp = value.getSurFaceTemp().get();


            if(isCleanSurTempData(currSurfaceTemp)) {
                if(minSurfaceTemp > currSurfaceTemp) {
                    minSurfaceTemp = currSurfaceTemp;
                    minSurfaceTempETW = "MIN-SURFACE-TEMP," + value.toString();
//                    toWrite.add(minSurfaceTempETW);
                }
                if(maxSurfaceTemp < currSurfaceTemp) {
                    maxSurfaceTemp = currSurfaceTemp;
                    maxSurfaceTempETW = "MAX-SURFACE-TEMP," + value.toString();
//                    toWrite.add(maxSurfaceTempETW);
                }
            }


            if (isCleanAirTempData(currAirTemp)) {
                if(minAirTemp > currAirTemp) {
                    minAirTemp = currAirTemp;
                    minAirTempETW = "MIN-AIR-TEMP," + value.toString();
//                    toWrite.add(minAirTempETW);
                }
                if(maxAirTemp < currAirTemp) {
                    maxAirTemp = currAirTemp;
                    maxAirTempETW = "MAX-AIR-TEMP," + value.toString();
//                    toWrite.add(maxAirTempETW);
                }
            }


        }

        System.out.println(minAirTempETW);
        System.out.println(maxAirTempETW);
        System.out.println(minSurfaceTempETW);
        System.out.println(maxSurfaceTempETW);


        toWrite.add(minSurfaceTempETW);
        toWrite.add(maxSurfaceTempETW);
        toWrite.add(minAirTempETW);
        toWrite.add(maxAirTempETW);

        StringBuilder sb = new StringBuilder();
        if(toWrite.size() > 0) {
            for(String e : toWrite) {
                sb.append(e);
            }
            context.write(new Text(), new Text(sb.toString()));
        }

    }


    private boolean isCleanAirTempData(Double currAirTemp) {
        if (currAirTemp >= 500) {
            return false;
        } else if (currAirTemp <= -500) {
            return false;
        }
        return true;
    }


    private boolean isCleanSurTempData(Double currSurfaceTemp) {
        if(currSurfaceTemp >= 500) {
            return false;
        }else if (currSurfaceTemp <= -500) {
            return false;
        }
        return true;
    }


}
