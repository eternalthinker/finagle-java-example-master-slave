/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vizury.videocache.common;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;

/**
 *
 * @author sankalpkulshrestha
 */
public class PropertyPlaceholder {

    private HashMap<String, String> propertyMap;
    private Properties props = null;
    
    public PropertyPlaceholder(String filename) {
        props = new Properties();
        try {
            //InputStream in = getClass().getResourceAsStream(filename);
            InputStream in = new FileInputStream(filename);
            props.load(in);
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void generatePropertyMap() {
        propertyMap = new HashMap<String, String>();
        for (Entry<Object, Object> e : props.entrySet()) {
            getPropertyMap().put((String) e.getKey(), (String) e.getValue());
        }
    }

    /**
     * @return the propertyMap
     */
    public HashMap<String, String> getPropertyMap() {
        return propertyMap;
    }
    
}
