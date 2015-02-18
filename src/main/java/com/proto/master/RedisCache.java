package com.proto.master;

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.util.CharsetUtil;

import scala.Option;
import scala.runtime.BoxedUnit;

import com.twitter.finagle.RedisClient;
import com.twitter.finagle.redis.Client;
import com.twitter.finagle.redis.util.StringToChannelBuffer;
import com.twitter.util.Future;

public class RedisCache {
    
    private Client redisClient;

    public RedisCache(String host, Integer port) {
        redisClient = new Client(RedisClient.newClient(host + ":" + port.toString()).toService());
    }
    
    public Future<Long> sAdd(String key, String value, long ttl) {
        ChannelBuffer keyBuf = StringToChannelBuffer.apply(key, CharsetUtil.UTF_8);
        ChannelBuffer valueBuf = StringToChannelBuffer.apply(value, CharsetUtil.UTF_8);
        List<ChannelBuffer> li= new ArrayList<ChannelBuffer>();
        li.add(valueBuf);
        redisClient.expire(keyBuf, ttl);
        return redisClient.sAdd(keyBuf, scala.collection.JavaConversions.asScalaBuffer(li).toList());
    }
    
    public Future<Long> sRem(String key, String value) {
        ChannelBuffer keyBuf = StringToChannelBuffer.apply(key, CharsetUtil.UTF_8);
        ChannelBuffer valueBuf = StringToChannelBuffer.apply(value, CharsetUtil.UTF_8);
        List<ChannelBuffer> li= new ArrayList<ChannelBuffer>();
        li.add(valueBuf);
        return redisClient.sRem(keyBuf, scala.collection.JavaConversions.asScalaBuffer(li).toList());
    }
    
    public Future<scala.collection.immutable.Set<ChannelBuffer>> sMembers(String key) {
        ChannelBuffer keyBuf = StringToChannelBuffer.apply(key, CharsetUtil.UTF_8);
        return redisClient.sMembers(keyBuf);
    }
    
    public Future<Option<ChannelBuffer>> get(String key) {
        ChannelBuffer keyBuf = StringToChannelBuffer.apply(key, CharsetUtil.UTF_8);
        return redisClient.get(keyBuf);
    }
    
    public Future<BoxedUnit> set(String key, String value, long ttl) {
        ChannelBuffer keyBuf = StringToChannelBuffer.apply(key, CharsetUtil.UTF_8);
        ChannelBuffer valueBuf = StringToChannelBuffer.apply(value, CharsetUtil.UTF_8);
        redisClient.expire(keyBuf, ttl);
        return redisClient.set(keyBuf, valueBuf);
    }

}
