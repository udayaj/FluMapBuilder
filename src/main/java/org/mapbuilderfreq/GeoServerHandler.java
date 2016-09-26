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

import it.geosolutions.geoserver.rest.GeoServerRESTPublisher;
import it.geosolutions.geoserver.rest.GeoServerRESTReader;
import it.geosolutions.geoserver.rest.decoder.RESTCoverageStoreList;
import it.geosolutions.geoserver.rest.decoder.utils.NameLinkElem;
import it.geosolutions.geoserver.rest.encoder.GSResourceEncoder;
import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.List;

public class GeoServerHandler {

    String url = "http://localhost:8080/geoserver";
    String user = "admin";
    String password = "geoserver";
    
    String workspace = "";
    String dataStore = "";
    String fileDirecetory = "G:\\NetbeansWorkspace\\Data\\in\\";
    String fileName = "20150801";

    /*
     String url = "http://geogis.bgsu.edu/geoserver";
     String user = "udaya";
     String password = "udaya123";
     */
    GeoServerRESTReader reader = null;
    GeoServerRESTPublisher publisher = null;

    public GeoServerHandler(String url, String user, String password) {
        try {
            this.url = url;
            this.user = user;
            this.password = password;

            reader = new GeoServerRESTReader(url, user, password);
            publisher = new GeoServerRESTPublisher(url, user, password);
        } catch (MalformedURLException ex) {
            System.err.println("MalformedURLException, connecting to GeoServer...");
            System.err.println(ex);
            System.exit(0);
        }
    }

    public void makeGeoServerLayer() {

        boolean exists = reader.existGeoserver();

        if (exists) {
            //Get workspaces
            List<String> list = reader.getWorkspaceNames();
            if (list.contains(workspace)) {
                try {

                    File tiff = new File(fileDirecetory + fileName + ".tiff");
                    double[] bbox = new double[]{-84.92, 38.38, -80.45, 42.12};

                    String coverageName = fileName;
                    publisher.publishGeoTIFF(workspace, dataStore, coverageName, tiff, "EPSG:4326", GSResourceEncoder.ProjectionPolicy.NONE, "flufreq", bbox);
                    
                    //GSLayerEncoder le = new GSLayerEncoder();
                    //le.setDefaultStyle("flufreq");
                    //publisher.configureLayer(workspace, fileName, le);
                } catch (FileNotFoundException ex) {
                    System.err.println("FileNotFoundException, shape file...");
                    System.err.println(ex);
                    System.exit(0);
                } catch (IllegalArgumentException ex) {
                    System.err.println("IllegalArgumentException, publishing GepTiff file...");
                    System.err.println(ex);
                    System.exit(0);
                }
            } else {
                System.err.println("Workspace does not exist in GeoServer!");
            }
        } else {
            System.err.println("No connection to GeoServer!");
        }
    }
    
    public boolean removeAllLayers(){
        RESTCoverageStoreList listCoeverageStores = reader.getCoverageStores(workspace);
       
        for (NameLinkElem nameLinkElem : listCoeverageStores) {
            publisher.removeCoverageStore(workspace,nameLinkElem.getName(), true);
        }
       return true;
    }
    
    public boolean removeLayer(){
       return publisher.removeCoverageStore(workspace,dataStore, true);
        //publisher.unpublishCoverage(workspace, dataStore, fileName);
        //return publisher.removeLayer(workspace, fileName);
    }    

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getWorkspace() {
        return workspace;
    }

    public void setWorkspace(String workspace) {
        this.workspace = workspace;
    }

    public String getDataStore() {
        return dataStore;
    }

    public void setDataStore(String dataStore) {
        this.dataStore = dataStore;
    }

    public String getFileDirecetory() {
        return fileDirecetory;
    }

    public void setFileDirecetory(String fileDirecetory) {
        this.fileDirecetory = fileDirecetory;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public GeoServerRESTReader getReader() {
        return reader;
    }

    public void setReader(GeoServerRESTReader reader) {
        this.reader = reader;
    }

    public GeoServerRESTPublisher getPublisher() {
        return publisher;
    }

    public void setPublisher(GeoServerRESTPublisher publisher) {
        this.publisher = publisher;
    }
    
}
