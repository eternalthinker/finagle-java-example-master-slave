package com.proto.master;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.jetty.util.ajax.JSON;
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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import scala.runtime.BoxedUnit;

import com.twitter.finagle.Http;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.http.HttpMuxer;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Time;
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
            //System.out.println("[GeneratorMaster] Request received: " + reqContent);

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
            if (type.equals("report")) {
                // Asynchronous call to process job completion response
                handleSlaveReport((Map<Object, Object>) jReq);
            }
            else if (type.equals("reports")) {
                // Process results
                JSONArray reports = (JSONArray) jReq.get("reports");
                for (Object obj : reports) {
                    JSONObject report = (JSONObject) obj;
                    System.out.println("Received report for job: " + report.get("job_id"));
                    handleSlaveReport((Map<Object, Object>) report);
                }
            }
            else if (type.equals("stats")) {
                jRes.put("commands_sent", stats.command);
                jRes.put("command_acks_received", stats.commandAck);
                jRes.put("commands_timedout", stats.commandTimeout);
                jRes.put("reports_received", stats.report);
            }

            jRes.put("type", "ack");
            HttpResponse response = createJsonResponse(jRes.toJSONString());

            return Future.value(response);
        }

    }

    /**
     * Scala closure to divide and assign jobs to slaves
     */
    private final int JOB_COUNT = 1;
    private final int SLAVE_THREADS = 5;
    private final int AVG_JOB_TIME = 10;
    private class GenerateVideos extends Function0<Object> {

        public Object apply() {
            // Assign jobs to slaves
            stats.startTime = System.currentTimeMillis();
            for (int i = 1; i <= JOB_COUNT; ++i) {
                generateVideo(i);
            }

            return new Object();
        }

    }

    private class Statistics {
        public long command = 0;
        public long commandAck = 0;
        public long commandTimeout = 0;
        public long report = 0;
        public boolean published = false;
        public long startTime = 0;

        public synchronized void incReport() {
            report++;
        }

        public synchronized void incCommandAck() {
            commandAck++;
        }
    }
    // End of inner classes

    private ListeningServer server;
    private RedisCache redisCache;
    private Statistics stats;
    private Service<HttpRequest, HttpResponse> client;
    private Random rand = new Random(); // Only for testing, remove later
    private static final String SLAVE_REQ_PATH = "/genvid/";
    private static final String SLAVE_PORT = "5000";

    public GeneratorMaster() {
        redisCache = new RedisCache("localhost", 7000);
        stats = new Statistics();
        client = ClientBuilder
                .safeBuild(ClientBuilder.get().codec(com.twitter.finagle.http.Http.get())
                        .hosts("localhost:" + SLAVE_PORT).hostConnectionLimit(500));
        // client = Http.newService("localhost:8001");
    }

    /**
     * Asynchronously sends initial job command to slave
     */
    private void generateVideo(Integer jobNum) {

        JSONObject jReq = new JSONObject();

        String pid = Integer.toString(rand.nextInt(1000));
        String imgUrl = "//img.cdn.origin/" + pid + ".jpg";
        //String jobID = UUID.randomUUID().toString();
        final String jobID = jobNum.toString();

        jReq.put("type", "command");
        jReq.put("job_id", jobID);
        jReq.put("pid", pid);
        jReq.put("img_url", imgUrl);

        HttpRequest request = createJsonRequest(jReq.toJSONString());

        System.out.println("[GeneratorMaster] Sending command for job: " + jobID);
        stats.command++;
        Future<HttpResponse> slaveAckF = client.apply(request);

        slaveAckF.addEventListener(new FutureEventListener<HttpResponse>() {

            public void onFailure(Throwable e) {
                System.out.println("[GeneratorMaster] Job command timed out for job: " + jobID);
                stats.commandTimeout++;
                System.out.println(e.getCause() + " : " + e.getMessage());
                //server.close();
                //System.exit(0);
            }

            public void onSuccess(HttpResponse response) {
                System.out.println("[GeneratorMaster] Job ack received for job: " + jobID + " -> " +
                        response.getContent().toString(UTF_8));
                stats.incCommandAck();
            }
        });
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
                //System.out.println("[GeneratorMaster] Cache update done. Completed job:" + jReq.get("job_id").toString());
                stats.incReport();
                if (!stats.published  && stats.commandAck == stats.report) {
                    stats.published = true;
                    StringBuilder content = new StringBuilder();
                    content.append("============= Master statistics =============");
                    content.append("\nCommands Sent: " + Long.toString(stats.command));
                    content.append("\nCommands Acked: " + Long.toString(stats.commandAck));
                    content.append("\nCommands Timed out: " + Long.toString(stats.commandTimeout));
                    content.append("\nJob Reports Received: " + Long.toString(stats.report));
                    long timeTaken = System.currentTimeMillis() - stats.startTime;
                    long x = timeTaken / 1000;
                    long seconds = x % 60;
                    x /= 60;
                    long  minutes = x % 60;
                    x /= 60;
                    long hours = x % 24;
                    String runTime = String.format("%d hr %d min, %d sec", 
                            hours, minutes, seconds);
                    content.append("\nTime taken: " + runTime);

                    x = JOB_COUNT * AVG_JOB_TIME / (JOB_COUNT >= SLAVE_THREADS? SLAVE_THREADS: 1);
                    seconds = x % 60;
                    x /= 60;
                    minutes = x % 60;
                    x /= 60;
                    hours = x % 24;
                    runTime = String.format("%d hr %d min, %d sec", 
                            hours, minutes, seconds);
                    content.append("\nAverage Time expected: " + runTime);
                    System.out.println(content.toString());
                }
            }
        });
    }

    private HttpRequest createJsonRequest(String content) {
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, SLAVE_REQ_PATH);
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

        InetSocketAddress addr = new InetSocketAddress(8000);
        server = Http.serve(addr, muxService);

        System.out.println("[GeneratorMaster] Started at " + addr.getAddress() + ":" + addr.getPort());
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
