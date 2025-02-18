package com.emolokov.faang_talk_flink.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class EnrichmentHttpService {

    private static final Pattern TEMP_METER_PATTERN = Pattern.compile("^temp-0*([0-9]+)$");
    private static final Pattern PRESS_METER_PATTERN = Pattern.compile("^press-0*([1-9][0-9]*)$");

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final HttpServer httpServer;

    public EnrichmentHttpService(){
        try {
            httpServer = HttpServer.create(new InetSocketAddress(80), 0);
        } catch (Exception e){
            log.error("Failed to start a Mock HTTP Server", e);
            throw new RuntimeException("Failed to start a Mock HTTP Server");
        }

        createContext();
    }

    private void createContext(){
        httpServer.createContext("/", httpExchange ->
        {
            String uriPath = httpExchange.getRequestURI().getPath();
            String enrichmentKey = uriPath.substring(uriPath.lastIndexOf('/') + 1);

            Matcher tempMatcher = TEMP_METER_PATTERN.matcher(enrichmentKey);
            Matcher pressMatcher = PRESS_METER_PATTERN.matcher(enrichmentKey);

            String enrichmentValue = "Unknown "+ enrichmentKey;
            if(tempMatcher.matches()){
                enrichmentValue = String.format("Temperature meter %s", tempMatcher.group(1));
            } else if(pressMatcher.matches()){
                enrichmentValue = String.format("Pressure meter %s", pressMatcher.group(1));
            }

            ObjectNode responseObject = JSON_MAPPER.createObjectNode();
            responseObject.put("value", enrichmentValue);

            sendResponse(httpExchange, 201, responseObject);
        });
    }

    public void start(){
        httpServer.start();
    }

    public void stop(){
        httpServer.stop(5);
    }

    private void sendResponse(HttpExchange httpExchange, int httpCode, ObjectNode response){

        final byte[] responseBytes;
        try {
            responseBytes = JSON_MAPPER.writeValueAsBytes(response);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize response json", e);
        }

        try {
            httpExchange.getResponseHeaders().add("Content-Type", "application/json");
            httpExchange.sendResponseHeaders(httpCode, responseBytes.length);

            OutputStream out = httpExchange.getResponseBody();
            out.write(responseBytes);
            out.flush();
            out.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to send response", e);
        }
    }
}
