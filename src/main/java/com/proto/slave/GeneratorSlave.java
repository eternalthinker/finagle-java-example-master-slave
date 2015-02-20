package com.proto.slave;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.InetSocketAddress;
import java.util.Map;
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
            // Simulated network delay
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
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
            
            JSONObject jRes = new JSONObject();
            
            String type = (String) jReq.get("type");
            if (type.equals("command")) {
                // Asynchronous call to start video generation
                generateVideo((Map) jReq);
                jRes.put("job_id", (String) jReq.get("job_id"));
                stats.incCommand();
            }
            
            if (type.equals("stats")) {
                jRes.put("commands_received_and_acked", stats.command);
                jRes.put("reports_sent", stats.report);
                jRes.put("reports_timedout", stats.reportTimedOut);
            }
            
            jRes.put("type", "ack");
            HttpResponse response = createJsonResponse(jRes.toJSONString());
            
            return Future.value(response);
        }

    }
    
    /**
     * Scala closure to execute the long running video generation task [blocking]
     */
    private class VideoGenerator extends Function0<Object> {
        private Map<Object, Object> jobInfo;
        
        public VideoGenerator(Map<Object, Object> jobInfo) {
            this.jobInfo = jobInfo;
        }

        public Object apply() {
            // Computation-heavy video generation work
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
            String jobID = (String) jobInfo.get("job_id");
            String pid = (String) jobInfo.get("pid");
            String imgUrl = (String) jobInfo.get("img_url");
            
            System.out.println("[GeneratorSlave] Video file generated for job: " + jobID);
            JobResult result = new JobResult();
            result.jobID = jobID;
            result.videoUrl = "//video.origin/bnid_" + pid + ".flv";
            result.pid = pid;
            
            return result;
        }

    }
    
    private class JobResult {
        public String pid;
        public String jobID;
        public String videoUrl;
    }
    
    private class SlaveStatistics {
        public long command = 0;
        public long report = 0;
        public long reportTimedOut = 0;
        public boolean published = false;
        
        public synchronized void incCommand() {
            command++;
        }
        
        public synchronized void incReport() {
            report++;
        }

        public void incReportTimedout() {
            reportTimedOut ++;
        }
    }
    // End of inner classes
    
    private static final int PID_COUNT = 3;
    
    private ExecutorServiceFuturePool futurePool;
    private ListeningServer server;
    private SlaveStatistics stats;
    
    public GeneratorSlave() {
        ExecutorService pool = Executors.newFixedThreadPool(20);
        futurePool = new ExecutorServiceFuturePool(pool);
        stats = new SlaveStatistics();
    }
    
    /**
     * Asynchronously runs video generation job in thread pool
     */
    private void generateVideo(Map<Object, Object> jobInfo) {
        Future<Object> videoGenResultF = futurePool.apply(new VideoGenerator(jobInfo));
        
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
        final JobResult result = (JobResult) videoInfo;
        
        jReq.put("type", "report");
        jReq.put("job_id", result.jobID);
        jReq.put("video_url", result.videoUrl);
        jReq.put("pid", result.pid);
        HttpRequest request = createJsonRequest(jReq.toJSONString());
        stats.incReport();
        
        Future<HttpResponse> masterAckF = client.apply(request);
        client.close();
        
        masterAckF.addEventListener(new FutureEventListener<HttpResponse>() {

            public void onFailure(Throwable e) {
                System.out.println("[GeneratorSlave] Failed to deliver report for job: " + result.jobID);
                stats.incReportTimedout();
            }

            public void onSuccess(HttpResponse arg0) {
            }
        });
        
        if (!stats.published && stats.command == stats.report) {
            stats.published = true;
            StringBuilder content = new StringBuilder();
            content.append("============= Slave statistics =============");
            content.append("\nCommands Received: " + Long.toString(stats.command));
            content.append("\nJob Reports Sent: " + Long.toString(stats.report));
            content.append("\nJob Reports Timed out: " + Long.toString(stats.reportTimedOut));
            System.out.println(content.toString());
        }
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
        HttpMuxer muxService = new HttpMuxer().withHandler("/", new VideoGenSlaveService());
        server = Http.serve(new InetSocketAddress("localhost", 8001), muxService);

        System.out.println("[GeneratorSlave] Started..");
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
        GeneratorSlave slaveServer = new GeneratorSlave();
        slaveServer.startServer();
        slaveServer.awaitServer();
    }

}
