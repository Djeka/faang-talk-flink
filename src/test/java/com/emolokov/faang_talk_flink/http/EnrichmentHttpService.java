package com.emolokov.faang_talk_flink.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.runtime.io.TestEvent;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;

@Slf4j
public class EnrichmentHttpService {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final HttpServer httpServer;

    private List<TestEvent> testEvents;

    public EnrichmentHttpService(){
        try {
            httpServer = HttpServer.create(new InetSocketAddress(80), 0);
        } catch (Exception e){
            log.error("Failed to start a Mock HTTP Server", e);
            throw new RuntimeException("Failed to start a Mock HTTP Server");
        }

        createContext();
    }

    public void setUsedTestEvents(List<TestEvent> testEvents){
        this.testEvents = testEvents;
    }

    private void createContext(){
        httpServer.createContext("/", httpExchange ->
        {
            String uriPath = httpExchange.getRequestURI().getPath();
            String enrichmentKey = uriPath.substring(uriPath.lastIndexOf('/'));

            String enrichmentKeyHash = "gen" + enrichmentKey.hashCode();

            ObjectNode responseObject = JSON_MAPPER.createObjectNode();
            responseObject.put("value", enrichmentKeyHash);

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
