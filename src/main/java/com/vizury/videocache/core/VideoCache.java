/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vizury.videocache.core;

import java.util.TimerTask;
import java.util.concurrent.ExecutorService;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.twitter.finagle.Service;
import com.vizury.videocache.common.PropertyPlaceholder;
import com.vizury.videocache.product.ProductFileReader;
import com.vizury.videocache.product.ProductReader;

/**
 *
 * @author sankalpkulshrestha
 */
public class VideoCache extends TimerTask {

    private final ProductReader productReader;
    private final ExecutorService pool;
    private final PropertyPlaceholder propsHolder;
    private final JedisPool jedisPool;
    private Service<HttpRequest, HttpResponse> client;

    public VideoCache(ExecutorService pool, PropertyPlaceholder propsHolder, Service<HttpRequest, HttpResponse> client) {
        this.pool = pool;
        this.client = client;
        this.propsHolder = propsHolder;
        propsHolder.generatePropertyMap();
        
        this.productReader = new ProductFileReader(propsHolder.getPropertyMap().get("campaignProductListLocation"));
       
        JedisPoolConfig jedisConfig = new JedisPoolConfig();
        jedisConfig.setMaxTotal(Integer.parseInt(propsHolder.getPropertyMap().get("maxJedisPoolSize")));
        jedisConfig.setTestOnBorrow(true);
        jedisConfig.setLifo(false);
        String redisHost = propsHolder.getPropertyMap().get("redisServer");
        int redisPort = Integer.parseInt(propsHolder.getPropertyMap().get("redisPort"));
        this.jedisPool = new JedisPool(jedisConfig, redisHost, redisPort);
    }

    public void refreshCache() {
        String[] campaignList = productReader.getCampaignList();
        if (campaignList != null) {
            Integer i = 1;
            for (String campaignId : campaignList) {
                pool.execute(new CampaignExecutor(campaignId, propsHolder.getPropertyMap(), jedisPool, client, i++));
            }
        }
    }

    @Override
    public void run() {
        refreshCache();
    }
}
