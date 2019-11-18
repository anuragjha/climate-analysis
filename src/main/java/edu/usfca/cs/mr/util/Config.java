package edu.usfca.cs.mr.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Config {

    private String dryingOutGeoHash;
    private String movingOutGeoHash;
    private String[] travelStartupGeoHashes;
    private String climateChartHash;

    public Config() {
    }

    public String getDryingOutGeoHash() {
        return dryingOutGeoHash;
    }

    public String getMovingOutGeoHash() {
        return movingOutGeoHash;
    }

    public String[] getTravelStartupGeoHashes() {
        return travelStartupGeoHashes;
    }

    public String getClimateChartHash() {
        return climateChartHash;
    }

    public static Config readConfig(String filename) {

        try {
            return readConfigInner(filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new Config();
    }

    /**
     * readConfig method reads the config file
     * @return
     */
    private static Config readConfigInner(String filename) throws IOException {
        Config config = null;

        File file = new File(filename);
        FileInputStream fis = null;
        fis = new FileInputStream(file);
        byte[] data = new byte[(int) file.length()];
        fis.read(data);
        fis.close();

        String str = new String(data, StandardCharsets.UTF_8);

        JsonParser parser = new JsonParser();
        JsonObject jObject = parser.parse(str).getAsJsonObject();

        config = new Gson().fromJson(jObject, Config.class);

        return config;
    }


//        /**
//         * @param args
//         * @throws IOException
//         */
//        public static void main(String[] args) throws IOException {
//
//            JsonReader jr = new JsonReader();
//            System.out.println(jr.readConfig().toString());
//
//        }

}

