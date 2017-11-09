package io.druid.emitter.influxdb;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceMetricEvent;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import io.druid.java.util.common.logger.Logger;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.HttpClient;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.util.Arrays;

public class InfluxdbEmitter implements Emitter {

    private final static Logger log = new Logger(InfluxdbEmitter.class);
    private HttpClient influxdbClient;
    private final InfluxdbEmitterConfig influxdbEmitterConfig;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final ScheduledExecutorService exec = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("InfluxdbEmitter-%s")
            .build());

    private final LinkedBlockingQueue<ServiceMetricEvent> eventsQueue;

    public InfluxdbEmitter(InfluxdbEmitterConfig influxdbEmitterConfig) {
        this.influxdbEmitterConfig = influxdbEmitterConfig;
        this.influxdbClient = HttpClientBuilder.create().build();
        this.eventsQueue = new LinkedBlockingQueue(influxdbEmitterConfig.getMaxQueueSize());
        log.info("constructing influxdb emitter");

    }

    public void start() {
        synchronized (started) {
            if (!started.get()) {
                exec.scheduleAtFixedRate(
                    new ConsumerRunnable(),
                    influxdbEmitterConfig.getFlushDelay(),
                    influxdbEmitterConfig.getFlushPeriod(),
                    TimeUnit.MILLISECONDS
                );
                started.set(true);
            }
        }
    }

    public void emit(Event event) {
        if (event instanceof ServiceMetricEvent)
        {
            ServiceMetricEvent metricEvent = (ServiceMetricEvent) event;
            try {
                eventsQueue.put(metricEvent);
            } catch (InterruptedException exception){
                log.error(exception.toString());
                Thread.currentThread().interrupt();
            }
        }
    }

    public void postToInflux(String payload){
        HttpPost post = new HttpPost(
                "http://" + influxdbEmitterConfig.getHostname()
                 + ":" + influxdbEmitterConfig.getPort()
                 + "/write?db=" + influxdbEmitterConfig.getDatabaseName()
                 + "&u=" + influxdbEmitterConfig.getInfluxdbUserName()
                 + "&p=" + influxdbEmitterConfig.getInfluxdbPassword()
        );

        post.setEntity(new StringEntity(payload, ContentType.DEFAULT_TEXT));
        post.setHeader("Content-Type","application/x-www-form-urlencoded");

        try {
            influxdbClient.execute(post);
        } catch(IOException ex) {
            log.info("request failed", ex.getMessage());
        } finally {
            post.releaseConnection();
        }
    }

    public String transformForInflux(ServiceMetricEvent event)
    {
        // transforms a service metric event into a String that can be posted to influxdb.
        // uses the tags,fields and measurement specified in the config file to comply with influxdb's line protocol

        String payload = getValue(influxdbEmitterConfig.getMeasurement(), event);

        if (influxdbEmitterConfig.getTags().size() == 0) { // checking if tags exist. if no then add a space after the measurement
            payload += " ";
        } else { // else append each tag-value pair to the string separated by commas
            String tagBuilder = ",";
            for (int i = 0; i < influxdbEmitterConfig.getTags().size()-1; i++){
                String tag = influxdbEmitterConfig.getTags().get(i);
                tagBuilder += tag + "=" + getValue(tag, event) + ",";
            }
            // a space instead of a comma must proceed the last tag-value pair
            String lastTag = influxdbEmitterConfig.getTags().get(influxdbEmitterConfig.getTags().size()-1);
            tagBuilder +=  lastTag + "=" + getValue(lastTag, event) + " ";
            payload += tagBuilder;
        }

        //generates the list of fields and field-values then appends to payload
        String fieldBuilder = "";
        for (int i = 0; i < influxdbEmitterConfig.getFields().size()-1; i++){
            String field = influxdbEmitterConfig.getFields().get(i);
            fieldBuilder += field + "=" + getValue(field, event) + ",";
        }
        // last field-value pair must be proceeded by a space and timestamp
        String lastField = influxdbEmitterConfig.getFields().get(influxdbEmitterConfig.getFields().size()-1);
        fieldBuilder +=  lastField + "=" + getValue(lastField, event);
        payload += fieldBuilder + " " + event.getCreatedTime().getMillis() * 1000000 + '\n'; // influxdb uses nano-second epoch timestamp

        return payload;
    }

    public String transformForInfluxSystems(ServiceMetricEvent event) {
        String[] parts = getValue("metric", event).split("/");
        String metric =  String.join(
            "_",
            Arrays.asList(
                Arrays.copyOfRange(
                    parts,
                    1,
                    parts.length-1
                )
            )
        );

        String payload = "druid_" + parts[0] + ",";

        payload += "service=" + getValue("service", event)
                    + ((parts.length == 2) ? "" : ",metric=druid_" + metric)
                    + ",hostname=" + getValue("host",event).split(":")[0]
                    + " druid_"
                    + parts[parts.length-1]+ "=" + getValue("value",event);

        return payload + " " + event.getCreatedTime().getMillis() * 1000000 + '\n';
    }

    public String getValue(String key, ServiceMetricEvent event) {
        switch (key){
            case "service":
                return event.getService();
            case "eventType":
                return event.getClass().getSimpleName();
            case "metric":
                return event.getMetric();
            case "feed":
                return event.getFeed();
            case "host":
                return event.getHost();
            case "value":
                return event.getValue().toString();
            default:
                return key;
        }
    }

    public void flush() throws IOException {
        if (started.get()) {
            transformAndSendToInfluxdb(eventsQueue);
        }
    }

    public void close() throws IOException {
        flush();
        log.info("Closing emitter io.druid.emitter.influxdb.InfluxdbEmitter");
        started.set(false);
        exec.shutdown();
    }

    public void transformAndSendToInfluxdb(LinkedBlockingQueue<ServiceMetricEvent> eventsQueue) {
        StringBuilder payload = new StringBuilder();
        int initialQueueSize = eventsQueue.size();
        for (int i =0; i < initialQueueSize; i++) {
            payload.append(transformForInfluxSystems(eventsQueue.poll()));
        }
        postToInflux(payload.toString());
    }

    private class ConsumerRunnable implements Runnable{
        @Override
        public void run() {
            transformAndSendToInfluxdb(eventsQueue);
        }
    }
}
