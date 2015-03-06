/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vizury.videocache.core;

import com.vizury.videocache.product.ProductDetail;
import java.util.List;
import java.util.Map;

/**
 *
 * @author sankalpkulshrestha
 */
public abstract class CacheConnect {

    private final String ip;
    private final int port;
    private int timeout;

    public CacheConnect(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public abstract String get(String key);

    public abstract void disconnect();

    public abstract Map<String, Object> getBulk(List<ProductDetail> productDetail, String typeKey);

    /**
     * @return the timeout
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * @param timeout the timeout to set
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
}
