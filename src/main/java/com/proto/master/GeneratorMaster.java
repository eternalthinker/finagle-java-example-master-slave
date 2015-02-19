package com.proto.master;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.twitter.finagle.Http;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Service;
import com.twitter.finagle.http.HttpMuxer;
import com.twitter.util.Await;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.TimeoutException;

public class GeneratorMaster {

    // Begin inner helper classes
    /**
     * Main server handler - awaits job success reports from slave
     */
    private class VideoGenMasterService extends Service<HttpRequest, HttpResponse> {

        @Override
        public Future<HttpResponse> apply(HttpRequest request) {
            String reqContent = request.getContent().toString(CharsetUtil.UTF_8);
            System.out.println("[GeneratorMaster] Request received: " + reqContent);

            // Parsing JSON request
            // Creating parser for each request, as static parser seem to throw error on consecutive requests
            JSONParser jsonParser = new JSONParser();
            JSONObject jReq = null;
            try {
                jReq = (JSONObject) jsonParser.parse(reqContent);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            
            String type = (String) jReq.get("type");
            if (type.equals("report")) {
                // Asynchronous call to process job completion response
                handleSlaveReport(request);
            }
            
            JSONObject jRes = new JSONObject();
            jRes.put("type", "ack");
            HttpResponse response = createJsonResponse(jRes.toJSONString());
            
            return Future.value(response);
        }

    }

    /**
     * Scala closure to divide and assign jobs to slaves
     */
    public class GenerateVideos extends Function0<Object> {

        public Object apply() {
            // Some pause to let the HTTP listening server up
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
            // Assign jobs to slaves
            for (int i = 0; i < 5; ++i) {
                generateVideo();
            }
            
            return new Object();
        }

    }
    
    /**
     * Asynchronously updates AdServing Redis cache
     */
    private class CacheUpdate extends Function<CdnInfo, Future<Object>> {
        
        private RedisCache redisCache;
        
        public CacheUpdate() {
            redisCache = new RedisCache("localhost", 7000);
        }

        public Future<Object> apply(CdnInfo cdnInfo) {
            return Future.value(new Object());
        }

    }
    
    /**
     * Scala closure to run the actual CDN push operation [blocking]
     */
    public class CdnUpdate extends Function0<CdnInfo> {

        public CdnInfo apply() {
            // Push video to CDN
            
            return new CdnInfo();
        }

    }
    
    private class CdnInfo {
        // Type for expected information returned after the CDN push
    }
    // End of inner classes

    public GeneratorMaster() {
    }
    
    /**
     * Asynchronously sends initial job command to slave
     */
    private void generateVideo() {
        Service<HttpRequest, HttpResponse> client = Http.newService("localhost:8001");
        JSONObject jReq = new JSONObject();
        jReq.put("type", "command");
        HttpRequest request = createJsonRequest(jReq.toJSONString());
        
        Future<HttpResponse> slaveAckF = client.apply(request);
    }
    
    /**
     * Asynchronously perform job assignment to slaves in thread pool
     */
    private void generateAllVideos() {
        ExecutorService pool = Executors.newFixedThreadPool(1);
        ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(pool);
        
        Future<Object> jobResultF = futurePool.apply(new GenerateVideos());
    }
    
    /**
     * Asynchronously performs follow up operations of video creation
     * @param request Job complete report from slave
     */
    private void handleSlaveReport(HttpRequest request) {
        Future<CdnInfo> cdnResponseF = updateCdn();
        
        Future<Object> cacheResponseF = cdnResponseF.flatMap(new CacheUpdate());
        
        cacheResponseF.addEventListener(new FutureEventListener<Object>() {

            public void onFailure(Throwable e) {
                e.printStackTrace();
            }

            public void onSuccess(Object cacheResult) {
                System.out.println("[GeneratorMaster] Cache update done. Sequence completed");
            }
        });
    }
    
    /**
     * Asynchronously performs CDN push in thread pool
     * @return Future of CDN push result
     */
    private Future<CdnInfo> updateCdn() {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(pool);
        
        Future<CdnInfo> cdnResultF = futurePool.apply(new CdnUpdate());
        return cdnResultF;
    }

    private HttpRequest createJsonRequest(String content) {
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
        ChannelBuffer buffer = ChannelBuffers.copiedBuffer(content, UTF_8);
        request.setContent(buffer);
        request.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=UTF-8");
        request.setHeader(HttpHeaders.Names.CONTENT_LENGTH, buffer.readableBytes());
        return request;
    }
    
    private HttpResponse createJsonResponse(String content) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        ChannelBuffer buffer = ChannelBuffers.copiedBuffer(content, UTF_8);
        response.setContent(buffer);
        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=UTF-8");
        response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, buffer.readableBytes());
        return response;
    }
    
    private void start() {
        HttpMuxer muxService = new HttpMuxer().withHandler("/", new VideoGenMasterService());
        ListeningServer server = Http.serve(new InetSocketAddress("localhost", 8023), muxService);

        System.out.println("[GeneratorMaster] Starting..");
        try {
            Await.ready(server);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        GeneratorMaster vidServer = new GeneratorMaster();
        
        // Calling the async job assignment thread before the blocking start() call
        // Now the below thread sleeps for a few seconds to let the start() call happen and HTTP server up
        // Is there a better way?
        vidServer.generateAllVideos();
        
        vidServer.start();
    }

}
