package edu.usfca.cs.mr.fireRelation;

import edu.usfca.cs.mr.extremes.ETWritable;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Line;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FireRelationMapper1
        extends Mapper<LongWritable, Text, Text, FireWritableFactors> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        try {
            if (isCleanData(line)) {
                String geoHash = Geohash.encode(
                        Float.parseFloat(Line.getLatitude(line)),
                        Float.parseFloat(Line.getLongitude(line)),
                        3
                );
                Double airTemp = Line.getAir_temperature(line);
                Double wind = Line.getWind_1_5(line);
                Double solarR = Line.getSolar_radiation(line);
                Double preci = Line.getPrecipitation(line);

                FireWritableFactors fwr = new FireWritableFactors()
                        .setGeohash(new Text(geoHash))
                        .setAirTemp(new DoubleWritable(airTemp))
                        .setWind(new DoubleWritable(wind))
                        .setSolarRadiation(new DoubleWritable(solarR))
                        .setPrecipitation(new DoubleWritable(preci));

                //System.out.println(fw.toString());

                context.write(new Text(geoHash), fwr);
            }
        } catch (Exception e) {
            System.out.println("Exception");
        }
    }


    private boolean isCleanData(String line) {

        String yearStr = Line.getUtc_date(line).substring(0,4);
        int year = Integer.parseInt(yearStr);
        if(year < 2006 || year >= 2016) {
            return false;
        }

        if(Integer.parseInt(Line.getWind_flag(line)) != 0) {
            return false;
        }else if(Line.getWind_1_5(line) < 0){
            return false;
        }

        if(Line.getAir_temperature(line) > 500){
            return false;
        }else if(Line.getAir_temperature((line)) < -500){
            return false;
        }
        if(Line.getPrecipitation(line) < 0){
            return false;
        }

        if(Integer.parseInt(Line.getSr_flag(line)) != 0) {
            return false;
        }else if(Line.getSolar_radiation(line) < 0){
            return false;
        }

        return true;
    }



}
