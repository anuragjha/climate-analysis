package edu.usfca.cs.mr.travelStartup;

import edu.usfca.cs.mr.util.Coordinates;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.NCDCWritable;
import edu.usfca.cs.mr.util.SpatialRange;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class TravelStartupReducer extends Reducer<Text, NCDCWritable,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<NCDCWritable> values, Context context)
            throws IOException, InterruptedException{
//        float bestComfortIndex = 0;
//        String geoHashForBestComfortIndex = "";
//        String bestLatitude = "";
//        String bestLongitude = "";

        HashMap<String,TotalTempAndHumidity> geohashToTotal = new HashMap<>(); // geohash,Month and total

        for(NCDCWritable ncdcWritable : values){
            String geohash = ncdcWritable.getGeohash().toString();
            String month = (ncdcWritable.getUtc_date().toString()).substring(4,6);
            float airTemp = Float.parseFloat(ncdcWritable.getAir_temperature().toString());
            float relativeHumidity = Float.parseFloat(ncdcWritable.getRelative_humidity().toString());
            String latitude = ncdcWritable.getLatitude().toString();
            String longitude = ncdcWritable.getLongitude().toString();

            String customKey = geohash+","+month;
//            if(geohashToTotal.containsKey(geohash)){
            if(geohashToTotal.containsKey(customKey)){
                TotalTempAndHumidity tth = geohashToTotal.get(customKey);
                tth.setAirtemp(tth.getAirtemp() + airTemp);
                tth.setHumidity(tth.getHumidity() + relativeHumidity);
                tth.setLatitude(latitude);
                tth.setLongitude(longitude);
                tth.setCount(1);
                geohashToTotal.put(customKey,tth);
            }else {
                TotalTempAndHumidity tth = new TotalTempAndHumidity(airTemp,relativeHumidity,1);
                geohashToTotal.put(customKey, tth);
            }
        }

//        for (String geo : geohashToTotal.keySet()){
//            TotalTempAndHumidity tth = geohashToTotal.get(geo);
//            float avgAirTemp =tth.getAirtemp()/tth.getCount();
//            float avgHumidity = tth.getHumidity()/tth.getCount();
//            tth.setComfortIndex((avgAirTemp + avgHumidity)/4);
//            if(tth.getComfortIndex() > 0 && tth.getComfortIndex() < 45){
//                if(tth.getComfortIndex() > bestComfortIndex) {
//                    bestComfortIndex = tth.getComfortIndex();
//                    geoHashForBestComfortIndex = geo;
//                    bestLatitude = tth.getLatitude();
//                    bestLongitude = tth.getLongitude();
//                }
//            }
//        }

        for (String geo : geohashToTotal.keySet()){
            TotalTempAndHumidity tth = geohashToTotal.get(geo);
            float avgAirTemp =tth.getAirtemp()/tth.getCount();
            float avgHumidity = tth.getHumidity()/tth.getCount();
            tth.setComfortIndex((avgAirTemp + avgHumidity)/4);
            float bestComfortIndex = tth.getComfortIndex();

            context.write(new Text(geo),new Text(","+ bestComfortIndex));

        }

        //Coordinates coordinates = Geohash.decodeHash(geoHashForBestComfortIndex).getCenterPoint();
//        String geoMonth = geoHashForBestComfortIndex + ","+bestLatitude +","+bestLongitude+","+key+",";
//        context.write(new Text(geoMonth),new Text(String.valueOf(bestComfortIndex)));
    }
}
