package edu.usfca.cs.mr.correlation;

import edu.usfca.cs.mr.pcc.RunningStatisticsND;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Line;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class CorrelationMapper extends Mapper<LongWritable, Text, Text, RunningStatisticsND> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        String line = value.toString();
        double solarRadiation = Line.getSolar_radiation(line);
        double getAir = Line.getAir_temperature(line);
        double precipitation = Line.getPrecipitation(line);
        double latitude = Double.parseDouble(Line.getLatitude(line));
        double longitude = Double.parseDouble(Line.getLongitude(line));
        double relativeHumidity = Line.getRelative_humidity(line);
        double wind = Line.getWind_1_5(line);
        double soilTemp = Line.getSoil_temperature_5(line);
        double soilMoisture = Line.getSoil_moisture_5(line);
        double wetness = Line.getWetness(line);
        double surfacetemp = Line.getSurface_temperature(line);

        //System.out.println("Precipitation : "+precipitation);
        //System.out.println("relativeHumidity : "+relativeHumidity);
        //System.out.println("wind speed : "+wind);
        //System.out.println("Soil Temp : "+soilTemp);
       // System.out.println("Soil Moisture : "+soilMoisture);
        //System.out.println("Wetness : "+wetness);

        String geohash = Geohash.encode(
                Float.parseFloat(Line.getLatitude(line)),
                Float.parseFloat(Line.getLongitude(line)),
                3
        );

        RunningStatisticsND nd = new RunningStatisticsND(9);
        nd.put(new double[]{solarRadiation,getAir,precipitation,relativeHumidity,wind, soilTemp, soilMoisture, wetness, surfacetemp});

            if(isCleanData(line)){
            context.write(new Text(String.valueOf(1)), nd);
        }
    }

    private boolean isCleanData(String line) {
        if(Integer.parseInt(Line.getWind_flag(line)) != 0) {
            return false;
        }else if(Line.getWind_1_5(line) < 0){
            return false;
        }
        if(Line.getWetness(line) < 0){
            return false;
        }
        if(Line.getSoil_temperature_5(line) > 500){
            return false;
        }else if(Line.getSoil_temperature_5(line) < -500){
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
        if(Line.getRelative_humidity(line) < 0){
            return false;
        }
        if(Line.getSoil_moisture_5(line) <0){
            return false;
        }
        if(Integer.parseInt(Line.getSr_flag(line)) != 0) {
            return false;
        }else if(Line.getSolar_radiation(line) < 0){
            return false;
        }
        if(Line.getSurface_temperature(line) > 500){
            return false;
        }else if(Line.getSurface_temperature(line) < -500){
            return false;
        }
        return true;
    }
}
