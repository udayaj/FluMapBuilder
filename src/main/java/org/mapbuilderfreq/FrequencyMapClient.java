/**
 *
 * MapBuilder
 *
 * Copyright (c) 2015 by Udaya
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package org.mapbuilderfreq;

import com.vividsolutions.jts.geom.Coordinate;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.configuration.*;
import org.apache.commons.daemon.*;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;

public class FrequencyMapClient implements Daemon {

    private static String DB_HOST = "";
    private static String DB_PORT = "";
    private static String DB_USER = "";
    private static String DB_PASSWORD = "";
    private static String DB_NAME = "";

    private static String GEOSERVER_IP = "";
    private static String GEOSERVER_PORT = "8080";
    private static String GEOSERVER_USER = "";
    private static String GEOSERVER_PASSWORD = "";

    private static String GEOSERVER_DATASTORE = "";
    private static String GEOSERVER_WORKSPACE = "";

    private static String SCORE_TYPE = "";
    private static float SCORE_THRESHOLD = 1.0f;
    private static Boolean ONLY_QUALIFIED = false;
    private static int NUM_DAYS_AGGREGATE = 5;
    private static float AGGREGATE_COEFFICIENT = 0.2f;

    private static String FILE_INPUT_DIR = "";
    private static String FILE_OUTPUT_DIR = "";

    //database parameters
    private static boolean FRESH_START = false;
    private static int REFRESH_INTERVAL = 60 * 60 * 1000;//in minutes
    private static int PAUSE_TIME = 10 * 1000;//in seconds
    private static int REFRESH_STATUS = 0; //0-No refresh, 1-Init (pause for 10s), 2-Refreshing (pause another 10s + refreshing time)
    private static Date LAST_MAP_DATE = null;

    private static Connection db = null;
    private static GeoServerHandler gsh = null;

    protected List<MsgData> list = new ArrayList<MsgData>();

    public static void main(String args[]) {
        System.out.println("MapBuilder started at " + new Date().toString());

        try {
            DefaultConfigurationBuilder builder = new DefaultConfigurationBuilder();
            File f = new File("config/config.xml");
            builder.setFile(f);
            CombinedConfiguration config = builder.getConfiguration(true);

            DB_HOST = config.getString("database.host");
            DB_PORT = config.getString("database.port");
            DB_USER = config.getString("database.user");
            DB_PASSWORD = config.getString("database.password");
            DB_NAME = config.getString("database.name");

            GEOSERVER_IP = config.getString("geoserver.geoserver-IP");
            GEOSERVER_PORT = config.getString("geoserver.geoserver-port");
            GEOSERVER_USER = config.getString("geoserver.geoserver-user");
            GEOSERVER_PASSWORD = config.getString("geoserver.geoserver-password");

            GEOSERVER_WORKSPACE = config.getString("geoserver.workspace");
            GEOSERVER_DATASTORE = config.getString("geoserver.datastore");

            SCORE_TYPE = config.getString("score-prams.type");
            SCORE_THRESHOLD = config.getFloat("score-prams.threshold");
            ONLY_QUALIFIED = config.getBoolean("score-prams.only-qualified");
            NUM_DAYS_AGGREGATE = config.getInt("score-prams.num-days-aggregate");
            AGGREGATE_COEFFICIENT = config.getFloat("score-prams.aggregate-coefficient");

            FILE_INPUT_DIR = config.getString("file-paths.input-path");
            FILE_OUTPUT_DIR = config.getString("file-paths.output-path");

            Class.forName("org.postgresql.Driver");
            String dbUrl = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;

            db = DriverManager.getConnection(dbUrl, DB_USER, DB_PASSWORD);

            String url = "http://" + GEOSERVER_IP + ":" + GEOSERVER_PORT + "/geoserver";
            gsh = new GeoServerHandler(url, GEOSERVER_USER, GEOSERVER_PASSWORD);
            gsh.setWorkspace(GEOSERVER_WORKSPACE);
            gsh.setFileDirecetory(FILE_OUTPUT_DIR);

            execute();
        } catch (ConfigurationException cex) {
            System.err.println("Error occurred while reading configurations....");
            System.err.println(cex);
            System.exit(0);
        } catch (ClassNotFoundException ex) {
            System.err.println("Database Driver not found....");
            System.err.println(ex);
            System.exit(0);
        } catch (SQLException ex) {
            System.err.println("Database Connection failed....");
            System.err.println(ex);
            System.exit(0);
        }
    }

    public static void execute() {
        try {
            while (true) {
                /**
                 * 1. Read System Status and load variables 2. DEEP START check
                 * 3. Go DEEP START / DAILY START 4. sc
                 */
                readSystemStatus();

                if (FRESH_START) {
                    //start genarating maps for all days
                    setRefreshStatus(1);//0-No refresh, 1-Init (pause for 10s), 2-Refreshing (pause another 10s + refreshing time)
                    Thread.sleep(PAUSE_TIME);
                    setRefreshStatus(2);
                    Thread.sleep(PAUSE_TIME);

                    //remove all exisiting flu layers in geoserver
                    //removeAllMapLayers();
                    //start processing for all days
                    doAllProcessing(null);

                    setFreshStart(false);
                    setRefreshStatus(0);
                    Thread.sleep(REFRESH_INTERVAL);//sleep till next houly job
                } else {
                    //generate daily map for this period
                    setRefreshStatus(1);//0-No refresh, 1-Init (pause for 10s), 2-Refreshing (pause another 10s + refreshing time)
                    Thread.sleep(PAUSE_TIME);
                    setRefreshStatus(2);
                    Thread.sleep(PAUSE_TIME);

                    //remove all exisiting flu layers in geoserver
                    //Date nextDate = DateUtils.addDays(LAST_MAP_DATE, 1);
                    //removeLayerByDate(nextDate);

                    //start processing for all days
                    doAllProcessing(LAST_MAP_DATE);

                    setRefreshStatus(0);
                    Thread.sleep(REFRESH_INTERVAL);//sleep till next houly job
                }

            }
        } catch (InterruptedException ex) {
            System.err.println("Thread sleeping failed....");
            System.err.println(ex);
            System.exit(0);
        }
    }

    public static void readSystemStatus() {
        //read status data from the database
        //assign to the variables
        try {
            String sysParamsQuery = "SELECT * FROM public.\"SysParams\" WHERE \"Id\" = 2";
            Statement st = db.createStatement();
            ResultSet rs = st.executeQuery(sysParamsQuery);

            while (rs.next()) {
                FRESH_START = rs.getBoolean(1);
                REFRESH_INTERVAL = ((int) rs.getShort(2)) * 60 * 1000;//in minutes
                PAUSE_TIME = ((int) rs.getShort(3)) * 1000;//in seconds
                REFRESH_STATUS = (int) rs.getShort(4); //0-No refresh, 1-Init (pause for 10s), 2-Refreshing (pause another 10s + refreshing time)

                Date tempDate = rs.getDate(5);
                if (tempDate == null) {
                    LAST_MAP_DATE = null;
                } else {
                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                    LAST_MAP_DATE = formatter.parse(formatter.format(tempDate));
                }
                break;
            }
        } catch (SQLException ex) {
            System.err.println("SysParams loading failed....");
            System.err.println(ex);
            System.exit(0);
        } catch (ParseException ex) {
            System.err.println("LAST_MAP_DATE parsing failed....");
            System.err.println(ex);
            System.exit(0);
        }
    }

//    public static void removeAllMapLayers() {
//        String url = "http://" + GEOSERVER_IP + ":" + GEOSERVER_PORT + "/geoserver";
//        GeoServerHandler gsh = new GeoServerHandler(url, GEOSERVER_USER, GEOSERVER_PASSWORD);
//        gsh.setWorkspace(GEOSERVER_WORKSPACE);
//        gsh.setDataStore(GEOSERVER_DATASTORE);
//        gsh.setFileDirecetory(FILE_OUTPUT_DIR);
//
//        gsh.removeAllLayers();
//    }
//
//    public static void removeLayerByDate(Date mapDate) {
//        String fileName = DateFormatUtils.format(mapDate, "yyyyMMMdd");
//        String url = "http://" + GEOSERVER_IP + ":" + GEOSERVER_PORT + "/geoserver";
//        GeoServerHandler gsh = new GeoServerHandler(url, GEOSERVER_USER, GEOSERVER_PASSWORD);
//        gsh.setWorkspace(GEOSERVER_WORKSPACE);
//        gsh.setDataStore(GEOSERVER_DATASTORE);
//        gsh.setFileDirecetory(FILE_OUTPUT_DIR);
//        gsh.setFileName(fileName);
//
//        gsh.removeLayer();
//    }

    public static void doAllProcessing(Date lastMapDate) {
        //Date today = new Date();
        Date today = getSoonestDate();
        Date nextDate = null;
        boolean deleteLayerByLayer = true;
        
        if (lastMapDate == null) {
            nextDate = getEarliestDate();            
            
            //delete all old layers at once
            gsh.removeAllLayers();
            //no need to delete again
            deleteLayerByLayer = false;
        } else {
            //nextDate = DateUtils.addDays(lastMapDate, 1);
            nextDate = lastMapDate;
            deleteLayerByLayer = true;
        }

        //Note: nextDate  have no time component, therefore always nextDate > today even if they are referring to the same day
        while (nextDate.before(today)) {
            //process for the selected day

            List<MsgData> dayList = getMapDataForDate(nextDate, SCORE_TYPE, SCORE_THRESHOLD, ONLY_QUALIFIED);

            List<DataPoint> newList = new ArrayList<>();
            if (!dayList.isEmpty()) {
                for (MsgData msgData : dayList) {
                    //set the Lat, Lon point from the exact point, OR polygon OR user location  
                    float lon = msgData.Longitude;
                    float lat = msgData.Latitude;
                    
                    String newOhioPlacePolygon = "POLYGON((-84.820309 38.403186,-84.820309 42.327133,-80.518626 42.327133,-80.518626 38.403186))";
                    //remove new Ohio place polygon for Ohio state, not it's diffrent
                    if(msgData.PlacePolygon.equals(newOhioPlacePolygon)){
                        continue;
                    }

                    if (lon == 0 && lat == 0) {
                        if (msgData.PlacePolygon != null && !msgData.PlacePolygon.isEmpty()) {
                            Coordinate coordinate = GeoToolsHandler.getCentroid(msgData.PlacePolygon);
                            lon = (float) coordinate.x;
                            lat = (float) coordinate.y;
                        } else {
                            System.err.println("No Lat/Lon OR PlacePolygon data found, MsgId: " + msgData.MsgId);
                            System.exit(0);
                        }
                    }
                    
                    //remove centroid for Ohio state
                    if(lon == -82.66945f && lat == 40.365158f){
                        continue;
                    }
                    
                    boolean pointAdded =  false;
                    for (int i = 0; i < newList.size(); i++) {
                        DataPoint temp = newList.get(i); 
                        
                        if(temp.Longitude == lon && temp.Latitude == lat){
                            temp.Score = temp.Score + 1;
                            newList.set(i, temp);
                            pointAdded =  true;
                            break;
                        }
                    }
                    
                    if(!pointAdded){
                        newList.add(new DataPoint(lon, lat, 1));
                    }                    
                }
            } else {
                //add no data points
                newList = getNoDataPointList();
            }

            //genarte the map
            generateMap(nextDate, newList, deleteLayerByLayer);

            //increment the date
            nextDate = DateUtils.addDays(nextDate, 1);
        }

        if (nextDate.after(today)) {
            nextDate = DateUtils.addDays(nextDate, -1);            
        }
        setLastMapDate(nextDate);
    }

    public static List<MsgData> getMapDataForDate(Date mapDate, String scoreType, float scoreThreshold, boolean onlyQualified) {
        List<MsgData> msgList = new ArrayList<>();

        //mapDate = DateUtils.addDays(mapDate, 1-NUM_DAYS_AGGREGATE);// add  -(NUM_DAYS_AGGREGATE-1) days
        
        for (int i = 0; i < NUM_DAYS_AGGREGATE; i++) {
            List<MsgData> tempList = new ArrayList<>();

            String lowerDate = DateFormatUtils.format(mapDate, "yyyy-MM-dd");
            String upperDate = DateFormatUtils.format(DateUtils.addDays(mapDate, 1), "yyyy-MM-dd");
            mapDate = DateUtils.addDays(mapDate, -1);

            float coefficient = (1 - (i * AGGREGATE_COEFFICIENT));

            try {
                String mapQuery = "SELECT \"Id\", \"Longitude\", \"Latitude\",\"PlacePolygon\",\"UserLocation\", \"NormalizedScore\", \"TagScore\", \"CombinedScore\" "
                        + "FROM public.\"ScoredMsg\" WHERE \"CreatedTime\" < '" + upperDate + "' AND \"CreatedTime\" >= '" + lowerDate + "' ";

                String scoreClause = "";
                switch (scoreType) {
                    case "normal":
                        scoreClause = "AND \"NormalizedScore\" >= " + scoreThreshold;
                        break;
                    case "tag":
                        scoreClause = "AND \"TagScore\" >= " + scoreThreshold;
                        break;
                    case "combined":
                        scoreClause = "AND \"CombinedScore\" >= " + scoreThreshold;
                        break;
                }
                mapQuery = mapQuery + scoreClause;

                if (onlyQualified) {
                    mapQuery = mapQuery + " AND \"IsQualified\" = true";
                }

                mapQuery = mapQuery + ";";

                Statement st = db.createStatement();
                ResultSet rs = st.executeQuery(mapQuery);

                while (rs.next()) {
                    MsgData d = new MsgData();

                    d.setMsgId(rs.getInt(1));
                    d.setLongitude(rs.getFloat(2));
                    d.setLatitude(rs.getFloat(3));
                    d.setPlacePolygon(rs.getString(4));
                    d.setUserLocation(rs.getString(5));
                    d.setNormalizedScore(rs.getFloat(6));
                    d.setTagScore(rs.getFloat(7));
                    d.setCombinedScore(rs.getFloat(8));

                    if (d.getNormalizedScore() <= 0) {
                        d.setNormalizedScore(d.getNormalizedScore() * coefficient);
                    }

                    if (d.getTagScore() <= 0) {
                        d.setTagScore(d.getTagScore() * coefficient);
                    }

                    if (d.getCombinedScore() <= 0) {
                        d.setCombinedScore(d.getCombinedScore() * coefficient);
                    }

                    tempList.add(d);
                }
            } catch (SQLException ex) {
                System.err.println("Map Query failed....");
                System.err.println(ex);
                System.exit(0);
            }

            msgList.addAll(tempList);
        }

        return msgList;
    }

    public static List<DataPoint> getNoDataPointList() {
        //use bounding box points for epty points
        //double[] bbox = new double[]{-84.92, 38.40, -80.52, 42.12};

        List<DataPoint> newList = new ArrayList<>();

        DataPoint p1 = new DataPoint(-84.92f, 38.40f, 0.0f);
        DataPoint p2 = new DataPoint(-80.52f, 42.12f, 0.0f);

        newList.add(p1);
        newList.add(p2);

        return newList;
    }

    public static void generateMap(Date mapDate, List<DataPoint> dataList, boolean deleteLayer) {
        /**
         * 1.Generate vector data 2.Generate tiff file 3.Make Data store if not
         * exist 4.Generate layer 5.Remove temporary files
         *
         */

        String fileName = DateFormatUtils.format(mapDate, "yyyyMMMdd");

        //make the shape file
        GeoToolsHandler gth = new GeoToolsHandler(FILE_OUTPUT_DIR, fileName);
        gth.setListData(dataList);
        gth.generateShapeFile();

        //make the tiff file
        GDALHandler gh = new GDALHandler(FILE_INPUT_DIR, FILE_OUTPUT_DIR, fileName);
        gh.generateGeoTiffFile();

        //make the geoserver store if not exist
        //String url = "http://" + GEOSERVER_IP + ":" + GEOSERVER_PORT + "/geoserver";
        //GeoServerHandler gsh = new GeoServerHandler(url, GEOSERVER_USER, GEOSERVER_PASSWORD);
        //gsh.setWorkspace(GEOSERVER_WORKSPACE);
        //gsh.setFileDirecetory(FILE_OUTPUT_DIR);
        gsh.setDataStore(GEOSERVER_DATASTORE + fileName);        
        gsh.setFileName(fileName);

        if (deleteLayer) {
            gsh.removeLayer();
        }

        gsh.makeGeoServerLayer();

        //remove temporary shape file
        gth.deleteFile();

        //remove temporary GeoTiff files
        gh.deleteFiles();
        
        //increment serial no
        incremnetMapSerialNo();
    }

    public static Date getSoonestDate() {
        //go through flu score data table
        Date soonest = null;
        try {
            String sysParamsQuery = "SELECT \"CreatedTime\" FROM public.\"ScoredMsg\" ORDER BY \"CreatedTime\" DESC LIMIT 1;";
            Statement st = db.createStatement();
            ResultSet rs = st.executeQuery(sysParamsQuery);

            while (rs.next()) {
                //Date tempDate = rs.getDate(1);
                //DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                //soonest = formatter.parse(formatter.format(tempDate));
                
                Timestamp ts = rs.getTimestamp(1);
                soonest = new Date(ts.getTime());
                
                break;
            }            
        } catch (SQLException ex) {
            System.err.println("Earliest Date loading failed....");
            System.err.println(ex);
            System.exit(0);
        }
        return soonest;
    }

    public static Date getEarliestDate() {
        //go through flu score data table
        Date earliestDate = null;
        try {
            String sysParamsQuery = "SELECT \"CreatedTime\" FROM public.\"ScoredMsg\" ORDER BY \"CreatedTime\" ASC LIMIT 1;";
            Statement st = db.createStatement();
            ResultSet rs = st.executeQuery(sysParamsQuery);

            while (rs.next()) {
                Date tempDate = rs.getDate(1);
                DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

                earliestDate = formatter.parse(formatter.format(tempDate));
                break;
            }
            return earliestDate;
        } catch (SQLException ex) {
            System.err.println("Earliest Date loading failed....");
            System.err.println(ex);
            System.exit(0);
        } catch (ParseException ex) {
            System.err.println("Date parsing failed....");
            System.err.println(ex);
            System.exit(0);
        }
        return null;
    }

     public static void incremnetMapSerialNo() {
        try {
            String updateQuery = "Update public.\"SysParams\" SET \"SerialNo\" = (\"SerialNo\" + 1) WHERE \"Id\" = 2;";
            PreparedStatement preparedStatement = db.prepareStatement(updateQuery);
            preparedStatement.executeUpdate();

        } catch (SQLException ex) {
            System.err.println("Incrementing Serial No failed (update query)...");
            System.err.println(ex);
            System.exit(0);
        }
    }
    
    public static void setRefreshStatus(int status) {
        try {
            String updateQuery = "Update public.\"SysParams\" SET \"RefreshStatus\" = ? WHERE \"Id\" = 2;";
            PreparedStatement preparedStatement = db.prepareStatement(updateQuery);
            preparedStatement.setShort(1, (short) status);
            preparedStatement.executeUpdate();

        } catch (SQLException ex) {
            System.err.println("RefreshStatus updating failed...");
            System.err.println(ex);
            System.exit(0);
        }
    }

    public static void setFreshStart(boolean freshStart) {
        try {
            String updateQuery = "Update public.\"SysParams\" SET \"FreshStart\" = ? WHERE \"Id\" = 2;";
            PreparedStatement preparedStatement = db.prepareStatement(updateQuery);
            preparedStatement.setBoolean(1, freshStart);
            preparedStatement.executeUpdate();

        } catch (SQLException ex) {
            System.err.println("FreshStart updating failed...");
            System.err.println(ex);
            System.exit(0);
        }
    }

    public static void setLastMapDate(Date lastMapDate) {
        java.sql.Date sqlDate = new java.sql.Date(lastMapDate.getTime());
        try {
            String updateQuery = "Update public.\"SysParams\" SET \"LastMapDate\" = ? WHERE \"Id\" = 2;";
            PreparedStatement preparedStatement = db.prepareStatement(updateQuery);
            preparedStatement.setDate(1, sqlDate);
            preparedStatement.executeUpdate();

        } catch (SQLException ex) {
            System.err.println("LastMapDate updating failed...");
            System.err.println(ex);
            System.exit(0);
        }
    }

    @Override
    public void init(DaemonContext dc) throws DaemonInitException, Exception {
        System.out.println("initializing ...");
    }

    @Override
    public void start() throws Exception {
        System.out.println("starting ...");
        main(null);
    }

    @Override
    public void stop() throws Exception {
        System.out.println("stopping ...");

        //stop_execution = true;
    }

    @Override
    public void destroy() {
        System.out.println("Stopped !");
    }
}
