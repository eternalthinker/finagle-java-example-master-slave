/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vizury.videocache.product;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sankalpkulshrestha
 */
public class ProductFileReader implements ProductReader {

    String directory;

    public ProductFileReader(String directory) {
        this.directory = directory;
    }

    @Override
    public List<ProductDetail> getProductDetailList(String campaignId) {
        List<ProductDetail> list = new ArrayList();
        File campaignFile = new File(directory + campaignId+".txt");
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(campaignFile), "UTF-8"));
            String line;
            String[] splitLine = new String[3];
            while ((line = bufferedReader.readLine()) != null) {
                splitLine = line.split("\t");
                try {
                    if (splitLine[0].trim().length() != 0 || splitLine[1].trim().length() != 0) {
                        list.add(new ProductDetail(Integer.parseInt(splitLine[0].trim()), Integer.parseInt(splitLine[1].trim()), splitLine[2].trim()));
                    }
                } catch (NumberFormatException ex) {
                }
            }
        } catch (FileNotFoundException | UnsupportedEncodingException ex) {
            Logger.getLogger(ProductFileReader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ProductFileReader.class.getName()).log(Level.SEVERE, null, ex);
        }
        return list;
    }

    @Override
    public String[] getCampaignList() {
        File folder = new File(directory);
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".txt");
            }
        };
        File[] listOfCampaignFiles = folder.listFiles(filter);
        String[] campaign = null;
        if (listOfCampaignFiles.length != 0) {
            campaign = new String[listOfCampaignFiles.length];
            int index = 0;
            for (File file : listOfCampaignFiles) {
                campaign[index++] = file.getName().replaceFirst("[.][^.]+$", "");
            }
        }
        return campaign;
    }
}
