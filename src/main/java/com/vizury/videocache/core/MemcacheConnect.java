/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vizury.videocache.core;

import com.vizury.videocache.product.ProductDetail;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;

/**
 *
 * @author sankalpkulshrestha
 */
class MemcacheConnect extends CacheConnect {

    MemcachedClient client;

    public MemcacheConnect(String ip, int port) {
        super(ip, port);
        try {
            client = new MemcachedClient(new ConnectionFactoryBuilder().setDaemon(true).setFailureMode(FailureMode.Retry).build(),AddrUtil.getAddresses(ip + ":" + port));
        } catch (IOException ex) {
            Logger.getLogger(MemcacheConnect.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public String get(String key) {
        Object value = null;
        Future<Object> future = client.asyncGet(key);
        try{
            value = future.get(getTimeout(),TimeUnit.MILLISECONDS);
        }catch(InterruptedException | ExecutionException | TimeoutException e){
            future.cancel(false);
        }
        return (String) value;
    }

    @Override
    public void disconnect() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Map<String, Object> getBulk(List<ProductDetail> productDetailList, String typeKey) {
        if(productDetailList!=null){
            List<String> key = new ArrayList<>();
            for(ProductDetail productDetail : productDetailList){
                key.add(productDetail.getNamespace()+typeKey+productDetail.getProductId());
            }
            return client.getBulk(key);
        }
        return null;
    }

}
