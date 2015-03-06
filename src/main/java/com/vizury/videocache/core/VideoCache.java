/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vizury.videocache.core;

import com.vizury.videocache.common.PropertyPlaceholder;
import com.vizury.videocache.product.ProductReader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author sankalpkulshrestha
 */
public class VideoCache {

    private final ProductReader productReader;
    private final ThreadPoolTaskExecutor taskExecutor;
    private final PropertyPlaceholder propertyMap;
    private final JedisPool jedisPool;

    public VideoCache(ProductReader productReader, ThreadPoolTaskExecutor taskExecutor, PropertyPlaceholder propertyMap, JedisPool jedisPool) {
        this.productReader = productReader;
        this.taskExecutor = taskExecutor;
        this.propertyMap = propertyMap;
        this.jedisPool = jedisPool;
        propertyMap.generatePropertyMap();
    }

    public void refreshCache() {
        String[] campaignList = productReader.getCampaignList();
        if (campaignList != null) {
            for (String campaignId : campaignList) {
                taskExecutor.execute(new CampaignExecutor(campaignId, propertyMap.getPropertyMap(), jedisPool));
            }
        }
    }
}
