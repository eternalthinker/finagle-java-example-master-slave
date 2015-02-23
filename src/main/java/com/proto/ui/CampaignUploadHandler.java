package com.proto.ui;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

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
import com.google.common.io.Files;

public class CampaignUploadHandler extends AbstractHandler {
    
    private static final MultipartConfigElement MULTI_PART_CONFIG = 
            new MultipartConfigElement(System.getProperty("java.io.tmpdir"));

    public void handle(String target, Request baseRequest, HttpServletRequest request,
            HttpServletResponse response) throws IOException, ServletException {

        // Begin response content
        StringBuilder content = new StringBuilder();
        content.append("<!DOCTYPE html>\n<html><body>");

        // Process upload request
        if ( request.getContentType() != null && 
                request.getContentType().startsWith( "multipart/form-data" )) {
            baseRequest.setAttribute(Request.__MULTIPART_CONFIG_ELEMENT, MULTI_PART_CONFIG);
            
            System.out.println("Request params found:");
            for (Part part : request.getParts()) {
                System.out.println("  * " + part.getName());
            }
            
            String campId = request.getParameter("camp-id");
            content.append("Details of submitted campaign: " + campId + "<br>");

            Part pidFilePart = request.getPart("pid-file");
            if (pidFilePart.getSize() > 0) {
                System.out.println("Saving pid file..");
                InputStream fileStream = pidFilePart.getInputStream(); 
                File dest = new File("pid/" + campId + ".txt");
                ByteStreams.copy(fileStream, Files.newOutputStreamSupplier(dest));
                content.append("Saved pid file..<br>");
            }
            
            Part videoFilePart = request.getPart("video-file");
            if (videoFilePart.getSize() > 0) {
                System.out.println("Saving video file..");
                InputStream fileStream = videoFilePart.getInputStream(); 
                File dest = new File("video/" + campId + ".flv");
                ByteStreams.copy(fileStream, Files.newOutputStreamSupplier(dest));
                content.append("Saved video file..<br>");
            }
            
            content.append("<hr />");
        }
        // End of upload request processing

        try {
            content.append(Files.toString(new File("static/upload_form.html"), Charsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
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
