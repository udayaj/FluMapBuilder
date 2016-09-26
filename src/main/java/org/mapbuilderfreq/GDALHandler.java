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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.geobricks.gdal.*;
import org.geobricks.gdal.constant.FORMAT;
import org.geobricks.gdal.general.GeoreferencedXExtents;
import org.geobricks.gdal.general.GeoreferencedYExtents;
import org.geobricks.gdal.grid.GDALGrid;
import org.geobricks.gdal.grid.InverseDistanceAlgo;
import org.geobricks.gdal.translate.OutputSize;
import org.geobricks.gdal.warp.GDALWarp;

public class GDALHandler {

    static String outputFileDirecetory = "G:\\NetbeansWorkspace\\Data\\out\\";
    static String inputFileDirecetory = "G:\\NetbeansWorkspace\\Data\\in\\";
    static String fileName = "20150801";
    static String clipFileName = "ohio_boundary";

    public GDALHandler(String inputDir, String outputDir, String file) {
        inputFileDirecetory = inputDir;
        outputFileDirecetory = outputDir;
        fileName = file;
    }
    
    public void generateGeoTiffFile() {
        this.generateInterpolationGeoTiffFile();
        this.clipGeoTiffFile();
    }

    private void generateInterpolationGeoTiffFile() {
        try {
            GDALConnector c = new GDALConnector();
            GDALGrid g = new GDALGrid();

            g.setInputFilepath(outputFileDirecetory + fileName + ".shp");
            g.setOutputFilepath(outputFileDirecetory + fileName + "_temp.tiff");
            g.setGeoreferencedXExtents(new GeoreferencedXExtents("-84.92", "-80.45"));
            g.setGeoreferencedYExtents(new GeoreferencedYExtents("38.38", "42.12"));

            InverseDistanceAlgo alg = new InverseDistanceAlgo();
            alg.setPower("2.0");
            alg.setSmoothing("0.1");
            g.setAlgorithm(alg);
            g.setOutputSize(new OutputSize("750", "750"));
            g.setOutputFormat(FORMAT.GTiff);
            g.setOutputBandsType("Float64");
            g.setLayerName(fileName);
            g.setzFieldName("number");
            g.setQuiet(true);

            List<String> l = c.invoke(g);
            //remove-print
            //TO-DO
            print(l);
        } catch (IOException e) {
            System.err.println("IOException, new Tiff file...");
            System.err.println(e);
            System.exit(0);
        } catch (Exception e) {
            System.err.println("Exception, new Tiff file...");
            System.err.println(e);
            System.exit(0);
        }
    }
    
    private void clipGeoTiffFile() {
        try {
            GDALConnector c = new GDALConnector();
            List<String> listFiles = new ArrayList<>();
            //listFiles.add(inputFileDirecetory + clipFileName + ".shp");//Boundary shape file
            //listFiles.add(outputFileDirecetory + fileName + "_temp.tiff");//origanl GeoTiff file            
            
            GDALWarp g = new GDALWarp(outputFileDirecetory + fileName + "_temp.tiff", outputFileDirecetory + fileName + ".tiff");
            g.setCutlineDatasource(inputFileDirecetory + clipFileName + ".shp");
            g.quiet(true);
            //set no data

            List<String> l = c.invoke(g);
            //remove-print
            print(l);
        } catch (IOException e) {
            System.err.println("IOException, new Tiff file...");
            System.err.println(e);
            System.exit(0);
        } catch (Exception e) {
            System.err.println("Exception, new Tiff file...");
            System.err.println(e);
            System.exit(0);
        }
    }

    public void print(List<String> l) {
        for (int i = 0; i < l.size(); i++) {
            System.out.format("[%03d]\t" + l.get(i) + "\n", i);
        }
        System.out.println();
    }

    public void deleteFiles() {
        try {
            Path path = FileSystems.getDefault().getPath(outputFileDirecetory, fileName + ".tiff");
            Path path2 = FileSystems.getDefault().getPath(outputFileDirecetory, fileName + "_temp.tiff");
            Files.deleteIfExists(path);
            Files.deleteIfExists(path2);
        } catch (IOException ex) {
            System.err.println("IOException, deleteing Tiff file...");
            System.err.println(ex);
            System.exit(0);
        }
    }

    public static String getInputFileDirecetory() {
        return inputFileDirecetory;
    }

    public static void setInputFileDirecetory(String inputFileDirecetory) {
        GDALHandler.inputFileDirecetory = inputFileDirecetory;
    }

    public static String getOutputFileDirecetory() {
        return outputFileDirecetory;
    }

    public static void setOutputFileDirecetory(String outputFileDirecetory) {
        GDALHandler.outputFileDirecetory = outputFileDirecetory;
    }

    public static String getFileName() {
        return fileName;
    }

    public static void setFileName(String fileName) {
        GDALHandler.fileName = fileName;
    }

}
