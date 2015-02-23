package com.proto.master;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.jboss.netty.buffer.ChannelBuffers.copiedBuffer;

import java.io.File;
import java.io.IOException;

import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.twitter.finagle.Service;
import com.twitter.util.Future;

public class UIService extends Service<HttpRequest, HttpResponse> {

    @Override
    public Future<HttpResponse> apply(HttpRequest request) {

        // Begin response content
        StringBuilder content = new StringBuilder();
        content.append("<!DOCTYPE html>\n<html><body>");

        System.out.println("[GeneratorMaster] Serving UI");

        try {
            content.append(Files.toString(new File("static/upload_form.html"), Charsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
        content.append("</body></html>");
        // End of response content

        HttpResponse response = new DefaultHttpResponse(
                request.getProtocolVersion(),
                HttpResponseStatus.OK
                );
        response.setContent(copiedBuffer(content.toString(), UTF_8));

        return Future.value(response);
    }

}
