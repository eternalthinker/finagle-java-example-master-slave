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
        /*File pidFile = (File) request.getAttribute("pid-file");

        if (pidFile == null || !pidFile.exists())
        {
            content.append("File does not exist");
        }
        else if ( pidFile.isDirectory() )
        {
            content.append("File is a directory");
        }
        else
        {
            File outputFile = new File("pid/" + request.getParameter("pid-file"));
            pidFile.renameTo(outputFile);
            content.append("File successfully uploaded.");
        }*/

        if ( request.getContentType() != null && 
                request.getContentType().startsWith( "multipart/form-data" )) {
            baseRequest.setAttribute(Request.__MULTIPART_CONFIG_ELEMENT, MULTI_PART_CONFIG);
            
            Collection<Part> parts = request.getParts();
            for (Part part : parts) {
                System.out.println("" + part.getName());
            }

            Part filePart = request.getPart("pid-file");

            if (filePart != null) {
                System.out.println("File part saving..");
                InputStream fileStream = filePart.getInputStream(); 
                ByteStreams.copy(fileStream, Files.newOutputStreamSupplier(new File("pid/pid1.txt")));
            }
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
