package com.emolokov.faang_talk_flink.functions;

import com.emolokov.faang_talk_flink.model.PipelineConfig;
import com.emolokov.faang_talk_flink.model.records.MeterRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.io.Serializable;
import java.util.List;

@Slf4j
public class EnrichmentFunction<R extends MeterRecord> extends RichAsyncFunction<R, R> implements Serializable {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final PipelineConfig pipelineConfig;

    public EnrichmentFunction(PipelineConfig pipelineConfig) {
        this.pipelineConfig = pipelineConfig;
    }

    @Override
    public void asyncInvoke(R record, ResultFuture<R> resultFuture) throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            // Define the target URL
            String getUrl = pipelineConfig.getEnrichmentEndpoint() + "/" + record.getMeterId();
            HttpGet request = new HttpGet(getUrl);

            // Execute the request and get the response
            String enrichmentValue = sendEnrichmentRequestWithAttempts(httpClient, request, 3); // give 3 attempts to get enrichment
            record.setMeterName(enrichmentValue);
        } catch (Throwable t) {
            log.warn("Failed to enrich: " + t.getMessage());
        } finally {
            // always complete the future with received and mutated record
            resultFuture.complete(List.of(record));
        }
    }

    private String sendEnrichmentRequestWithAttempts(CloseableHttpClient httpClient, HttpGet request, int attemptsBudget){
        if(attemptsBudget == 0){
            throw new RuntimeException("Failed to get enrichment");
        }

        attemptsBudget = attemptsBudget - 1;

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String responseBody = EntityUtils.toString(response.getEntity());

            if(response.getCode() >= 200 && response.getCode() < 300){
                // successful response - update the enrichment value
                return JSON_MAPPER.readTree(responseBody).get("value").asText();
            } else {
                // failed enrichment attempt, try again
                return sendEnrichmentRequestWithAttempts(httpClient, request, attemptsBudget);
            }
        } catch (Exception e) {
            // failed enrichment attempt, try again
            return sendEnrichmentRequestWithAttempts(httpClient, request, attemptsBudget);
        }
    }

    @Override
    public void timeout(R record, ResultFuture<R> resultFuture) throws Exception {
        resultFuture.complete(List.of(record)); // skip enrichment
    }
}
