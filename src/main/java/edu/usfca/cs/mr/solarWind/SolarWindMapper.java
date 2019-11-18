package edu.usfca.cs.mr.solarWind;

import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Line;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SolarWindMapper extends Mapper<LongWritable, Text,Text,SolarWindDetails> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        double solar = Line.getSolar_radiation(line);
        double windspeed =  Line.getWind_1_5(line);
        float latitude = Float.parseFloat(Line.getLatitude(line));
        float longitude = Float.parseFloat( Line.getLongitude(line));

        SolarWindDetails details = new SolarWindDetails();
        details.set(new DoubleWritable(solar),new DoubleWritable(windspeed),new FloatWritable(latitude),new FloatWritable(longitude));

        String geohash = Geohash.encode(
                Float.parseFloat(Line.getLatitude(line)),
                Float.parseFloat(Line.getLongitude(line)),
                3
        );
        if(isCleanData(line)) {
            context.write(new Text(geohash), details);
        }
    }

    private boolean isCleanData(String line) {
        if(Integer.parseInt(Line.getWind_flag(line)) != 0) {
            return false;
        }else if(Line.getWind_1_5(line) < 0){
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
