package io.druid.emitter.influxdb;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.collect.ImmutableSortedSet;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.core.Event;
import io.druid.java.util.emitter.service.ServiceMetricEvent;
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
import java.util.regex.Pattern;


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
            log.info(ex.toString());
        } finally {
            post.releaseConnection();
        }
    }

    public String transformForInfluxSystems(ServiceMetricEvent event) {
        // split Druid metric on slashes and join middle parts (if any) with "_"
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
                    + ",hostname=" + getValue("host",event).split(":")[0];


        ImmutableSortedSet<String> dimNames = ImmutableSortedSet.copyOf(event.getUserDims().keySet());
        for (String dimName : dimNames) {
            payload += "," + dimName + "=" + sanitize(String.valueOf(event.getUserDims().get(dimName)));
        }

        payload += " druid_" + parts[parts.length-1]+ "=" + getValue("value",event);

        return payload + " " + event.getCreatedTime().getMillis() * 1000000 + '\n';
    }

    protected static String sanitize(String namespace)
    {
        Pattern DOT_OR_WHITESPACE = Pattern.compile("[\\s]+|[.]+");
        String sanitizedNamespace = DOT_OR_WHITESPACE.matcher(namespace).replaceAll("_");
        return sanitizedNamespace;
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
