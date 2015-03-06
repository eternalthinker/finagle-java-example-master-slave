/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vizury.videocache.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

/**
 *
 * @author sankalpkulshrestha
 */
public class PropertyPlaceholder extends PropertyPlaceholderConfigurer {

    Properties props;
    private HashMap<String, String> propertyMap;

    @Override
    protected Properties mergeProperties() throws IOException {
        props = super.mergeProperties();
        return props;
    }

    public void generatePropertyMap() {
        propertyMap = new HashMap();
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
