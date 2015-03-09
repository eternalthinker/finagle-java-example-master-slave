package com.vizury.videocache.ui;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.vizury.videocache.common.PropertyPlaceholder;

public class CampaignUploadHandler extends AbstractHandler {

    private static final MultipartConfigElement MULTI_PART_CONFIG = 
            new MultipartConfigElement(System.getProperty("java.io.tmpdir"));
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd.HH.mm");
    private final String uploadDir;

    public CampaignUploadHandler() {
        String propertiesPath = "/videogenmaster.properties";
        PropertyPlaceholder propsHolder = new PropertyPlaceholder(propertiesPath);
        propsHolder.generatePropertyMap();
        Map<String, String> props = propsHolder.getPropertyMap();
        this.uploadDir = props.get("campaignProductListLocation");
    }


    public void handle(String target, Request baseRequest, HttpServletRequest request,
            HttpServletResponse response) throws IOException, ServletException {

        // Begin response content
        StringBuilder content = new StringBuilder();
        content.append("<!DOCTYPE html>\n<html><body><head>");
        content.append("<title>Video Campaign Upload</title></head>");

        // Process upload request
        if ( request.getContentType() != null && 
                request.getContentType().startsWith( "multipart/form-data" )) {

            baseRequest.setAttribute(Request.__MULTIPART_CONFIG_ELEMENT, MULTI_PART_CONFIG);

            System.out.println("Request params found:");
            for (Part part : request.getParts()) {
                System.out.println("  * " + part.getName());
            }

            String campId = request.getParameter("camp-id");
            content.append("Details of submitted campaign: " + campId + "<br> ");

            Date now = new Date();
            String dirName = uploadDir + "/" + dateFormat.format(now);
            dirName = uploadDir;

            File destDir = new File(dirName);
            if (! destDir.exists()) {
                try{
                    destDir.mkdir();
                } catch(SecurityException se){
                    System.out.println(se.getMessage());
                }        
            }

            Part pidFilePart = request.getPart("pid-file");
            if (pidFilePart.getSize() > 0) {
                System.out.println("Saving pid file..");
                InputStream fileStream = pidFilePart.getInputStream(); 
                // Write to temp file ?
                File dest = new File(dirName + "/" + campId + "_pid.txt");
                ByteStreams.copy(fileStream, Files.newOutputStreamSupplier(dest));
                content.append("Saved pid file.<br> ");
            }

            Part videoFilePart = request.getPart("video-file");
            if (videoFilePart.getSize() > 0) {
                System.out.println("Saving video file..");
                InputStream fileStream = videoFilePart.getInputStream(); 
                File dest = new File(dirName + "/" + campId + "_video.flv");
                ByteStreams.copy(fileStream, Files.newOutputStreamSupplier(dest));
                content.append("Saved video file.<br>");
            }

            content.append("<hr />");

            // Optionally update the Master Video Generator service
            /*System.out.println("Sending data to Master..");
            URL url = new URL("http://localhost:8000/");
            URLConnection UrlConnObj = url.openConnection();
            HttpURLConnection huc = (HttpURLConnection) UrlConnObj;
            huc.setRequestMethod("POST");
            huc.setDoOutput(true);
            huc.setDoInput(true);
            huc.setUseCaches(false);
            huc.setDefaultUseCaches(false);
            huc.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            String data = "{\"type\":\"notify\"}";
            OutputStream output;
            try {
                output = huc.getOutputStream();
                output.write(data.getBytes("UTF-8"));
            } catch (Exception e) {
                e.printStackTrace();
            }
            InputStream jRes = huc.getInputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(jRes));
            String inputLine;
            while ((inputLine = in.readLine()) != null) 
                System.out.println(inputLine);
            in.close();*/
        }
        // End of upload request processing


        try {
            InputStream in = getClass().getResourceAsStream("/static/upload_form.html");
            final InputStreamReader inr = new InputStreamReader(in); 
            content.append(CharStreams.toString(inr));
        } catch (Exception e) {
            System.out.println(e.getCause() + " : " + e.getMessage());
        }

        content.append("</body></html>");
        // End of response content

        response.getWriter().println(content);

        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
    }


    public static void main(String[] args) {
        Server server = new Server(8080);

        server.setHandler(new CampaignUploadHandler());

        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            server.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
