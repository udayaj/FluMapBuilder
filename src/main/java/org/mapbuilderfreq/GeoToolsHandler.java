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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class GeoToolsHandler {

    String fileDirecetory = "G:\\NetbeansWorkspace\\Data\\in\\";
    String fileName = "20150801";
    List<DataPoint> listData;

    public GeoToolsHandler(String dir, String file) {
        fileDirecetory = dir;
        fileName = file;
    }

    public void generateShapeFile() {
        try {
            final SimpleFeatureType TYPE = DataUtilities.createType("Location",
                    "the_geom:Point:srid=4326," + // <- the geometry attribute: Point type
                    "number:Float" // a number attribute, not Integer
            );

            List<SimpleFeature> features = new ArrayList<SimpleFeature>();

            GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
            SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(TYPE);

            for (DataPoint dataPoint : listData) {

                float latitude = dataPoint.Latitude;
                float longitude = dataPoint.Longitude;
                float number = dataPoint.Score;

                /* Longitude (= x coord) first ! */
                Point point = geometryFactory.createPoint(new Coordinate(longitude, latitude));

                featureBuilder.add(point);
                featureBuilder.add(number);
                SimpleFeature feature = featureBuilder.buildFeature(null);
                features.add(feature);
            }

            File newFile = new File(getFileDirecetory() + getFileName() + ".shp");

            ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();

            Map<String, Serializable> params = new HashMap<String, Serializable>();
            params.put("url", newFile.toURI().toURL());
            params.put("create spatial index", Boolean.TRUE);

            ShapefileDataStore newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
            newDataStore.createSchema(TYPE);

            /*
             * Write the features to the shapefile
             */
            Transaction transaction = new DefaultTransaction("create");

            String typeName = newDataStore.getTypeNames()[0];
            SimpleFeatureSource featureSource = newDataStore.getFeatureSource(typeName);
            SimpleFeatureType SHAPE_TYPE = featureSource.getSchema();
            /*
             * The Shapefile format has a couple limitations:
             * - "the_geom" is always first, and used for the geometry attribute name
             * - "the_geom" must be of type Point, MultiPoint, MuiltiLineString, MultiPolygon
             * - Attribute names are limited in length
             * - Not all data types are supported (example Timestamp represented as Date)
             *
             * Each data store has different limitations so check the resulting SimpleFeatureType.
             */
            //System.out.println("SHAPE:" + SHAPE_TYPE);

            if (featureSource instanceof SimpleFeatureStore) {
                SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
                /*
                 * SimpleFeatureStore has a method to add features from a
                 * SimpleFeatureCollection object, so we use the ListFeatureCollection
                 * class to wrap our list of features.
                 */
                SimpleFeatureCollection collection = new ListFeatureCollection(TYPE, features);
                featureStore.setTransaction(transaction);
                try {
                    featureStore.addFeatures(collection);
                    transaction.commit();
                    transaction.close();
                } catch (IOException IOExc) {
                    transaction.rollback();
                    transaction.close();
                    
                    System.err.println("IOException, writing new shape file...");
                    System.err.println(IOExc);
                    System.exit(0); // success!
                }
                
            } else {
                System.err.println(typeName + " does not support read/write access");
                System.exit(1);
            }

        } catch (SchemaException ex) {
            System.err.println("SchemaException, new shape file...");
            System.err.println(ex);
            System.exit(0);
        } catch (MalformedURLException ex) {
            System.err.println("Malformed URL, new shape file...");
            System.err.println(ex);
            System.exit(0);
        } catch (IOException ex) {
            System.err.println("IO Exception, new shape file...");
            System.err.println(ex);
            System.exit(0);
        }
    }
    
    public void deleteFile(){
        try {
            Path path = FileSystems.getDefault().getPath(fileDirecetory, fileName + ".shp");
            Files.deleteIfExists(path);
            
            path = FileSystems.getDefault().getPath(fileDirecetory, fileName + ".shx");
            Files.deleteIfExists(path);
            
            path = FileSystems.getDefault().getPath(fileDirecetory, fileName + ".prj");
            Files.deleteIfExists(path);
            
            path = FileSystems.getDefault().getPath(fileDirecetory, fileName + ".dbf");
            Files.deleteIfExists(path);
            
            path = FileSystems.getDefault().getPath(fileDirecetory, fileName + ".fix");
            Files.deleteIfExists(path);
        } catch (IOException ex) {
            System.err.println("IOException, deleteing shape file...");
            System.err.println(ex);
            System.exit(0);
        }
    }
    
    public static Coordinate getCentroid(String strPolygon){
        try {
            WKTReader reader = new WKTReader();
            Pattern p = Pattern.compile("[-\\.0-9\\s]+");
            Matcher m = p.matcher(strPolygon);
            if(m.find()){
                strPolygon =  strPolygon.replace("))", ",") +  m.group()+ "))";
            }
            
            Geometry shape = reader.read(strPolygon);
            Coordinate cord = shape.getCentroid().getCoordinate();
            return cord;
            //System.out.println("X: " + cord.x);
            //System.out.println("Y: " + cord.y);
        } catch (ParseException ex) {
            System.err.println("ParseException, calculating centroid for polygon...");
            System.err.println(ex);
            System.exit(0);
        }        
        return null;
    }
    
    public String getFileDirecetory() {
        return this.fileDirecetory;
    }

    public void setFileDirecetory(String fileDirecetory) {
        this.fileDirecetory = fileDirecetory;
    }

    public String getFileName() {
        return this.fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public List<DataPoint> getListData() {
        return this.listData;
    }

    public void setListData(List<DataPoint> listData) {
        this.listData = listData;
    }
}
