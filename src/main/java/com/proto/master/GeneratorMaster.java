package com.proto.master;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
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
                handleSlaveReport((Map<Object, Object>) jReq);
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
            // Assign jobs to slaves
            for (int i = 0; i < 5; ++i) {
                generateVideo();
            }
            
            return new Object();
        }

    }
    // End of inner classes

    private ListeningServer server;
    private RedisCache redisCache;
    private Random rand = new Random(); // Only for testing, remove later
    
    public GeneratorMaster() {
        redisCache = new RedisCache("localhost", 7000);
    }
    
    /**
     * Asynchronously sends initial job command to slave
     */
    private void generateVideo() {
        Service<HttpRequest, HttpResponse> client = Http.newService("localhost:8001");
        JSONObject jReq = new JSONObject();
        
        String pid = Integer.toString(rand.nextInt(1000));
        String imgUrl = "//img.cdn.origin/" + pid + ".jpg";
        String jobID = UUID.randomUUID().toString();

        jReq.put("type", "command");
        jReq.put("job_id", jobID);
        jReq.put("pid", pid);
        jReq.put("img_url", imgUrl);
        
        HttpRequest request = createJsonRequest(jReq.toJSONString());
        
        System.out.println("[GeneratorMaster] Sending command for job: " + jobID);
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
    private void handleSlaveReport(final Map<Object, Object> jReq) {
        
        // Update Redis Cache
        Future<Object> cacheResponseF = Future.value(new Object());
        // redisCache.sAdd(key, jReq.get("video_url"), ttl);
        
        cacheResponseF.addEventListener(new FutureEventListener<Object>() {

            public void onFailure(Throwable e) {
                e.printStackTrace();
            }

            public void onSuccess(Object cacheResult) {
                System.out.println("[GeneratorMaster] Cache update done. Sequence completed for job:" + jReq.get("job_id").toString());
            }
        });
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
    
    private void startServer() {
        HttpMuxer muxService = new HttpMuxer().withHandler("/", new VideoGenMasterService());
        server = Http.serve(new InetSocketAddress("localhost", 8000), muxService);

        System.out.println("[GeneratorMaster] Started..");
    }
    
    private void awaitServer() {
        try {
            Await.ready(server);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        GeneratorMaster masterServer = new GeneratorMaster();
        
        masterServer.startServer();
        
        // Starting the job assignment thread
        // Once this call is made, command requests are eventually sent out to slaves, 
        // and we can start expecting job status reports (not immediately in practice).
        masterServer.generateAllVideos();
        
        masterServer.awaitServer();
    }

}
