package com.proto.slave;

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
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.TimeoutException;

public class GeneratorSlave {
    
    // Begin inner helper classes
    /**
     * Main server handler - listens to job commands from master
     */
    private class VideoGenSlaveService extends Service<HttpRequest, HttpResponse> {

        @Override
        public Future<HttpResponse> apply(HttpRequest request) {
            String reqContent = request.getContent().toString(CharsetUtil.UTF_8);
            System.out.println("[GeneratorSlave] Request received: " + reqContent);

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
            if (type.equals("command")) {
                // Asynchronous call to start video generation
                generateVideo();
            }
            
            HttpResponse response = createJsonResponse("[payload]");
            
            return Future.value(response);
        }

    }
    
    /**
     * Scala closure to execute the long running video generation task [blocking]
     */
    private class VideoGenerator extends Function0<Object> {

        public Object apply() {
            // Computation-heavy video generation work
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("[GeneratorSlave] Video file generated");
            
            return new Object();
        }

    }
    // End of inner classes
    
    private ExecutorServiceFuturePool futurePool;
    
    public GeneratorSlave() {
        ExecutorService pool = Executors.newFixedThreadPool(10);
        futurePool = new ExecutorServiceFuturePool(pool);
    }
    
    /**
     * Asynchronously runs video generation job in thread pool
     */
    private void generateVideo() {
        Future<Object> videoGenResultF = futurePool.apply(new VideoGenerator());
        
        videoGenResultF.addEventListener(new FutureEventListener<Object>() {

            public void onFailure(Throwable e) {
                e.printStackTrace();
            }

            public void onSuccess(Object obj) {
                notifySuccess(obj);
            }
            
        });
    }

    /**
     * Notify successful video generation to master
     * @param videoInfo Info on created video
     */
    private void notifySuccess(Object videoInfo) {
        Service<HttpRequest, HttpResponse> client = Http.newService("localhost:8000");
        JSONObject jReq = new JSONObject();
        jReq.put("type", "report");
        HttpRequest request = createJsonRequest(jReq.toJSONString());
        
        Future<HttpResponse> masterAckF = client.apply(request);
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
        HttpMuxer muxService = new HttpMuxer().withHandler("/", new VideoGenSlaveService());
        ListeningServer server = Http.serve(new InetSocketAddress("localhost", 8001), muxService);

        System.out.println("[GeneratorSlave] Starting..");
        try {
            Await.ready(server);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        GeneratorSlave slaveServer = new GeneratorSlave();
        slaveServer.start();
    }

}
