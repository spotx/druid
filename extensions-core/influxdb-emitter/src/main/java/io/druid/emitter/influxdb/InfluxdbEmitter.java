package io.druid.emitter.influxdb;

//not sure which imports will be needed - some taken from AmbariMetricsEmitterModule

//import com.fasterxml.jackson.databind.Module;
//import com.google.inject.Binder;
//import io.druid.guice.JsonConfigProvider;
//import io.druid.initialization.DruidModule;
//import java.util.Collections;
//import java.util.List;
//import com.fasterxml.jackson.databind.node.BinaryNode;
//import com.mashape.unirest.http.HttpResponse;
//import com.mashape.unirest.http.JsonNode;
//import com.mashape.unirest.http.Unirest;
//import com.mashape.unirest.http.exceptions.UnirestException;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceMetricEvent;
import java.io.IOException;

//import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import java.time.Instant;
//import java.util.Locale;
//import com.mashape.unirest.*;
//import org.json.*;
//import org.apache.http.NameValuePair;
//import org.apache.http.message.BasicNameValuePair;
//import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.HttpClient;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
//import java.util.ArrayList;
//import java.util.List;


public class InfluxdbEmitter implements Emitter{

    private final static Logger log = new Logger(InfluxdbEmitter.class);
    private HttpClient client;

    public InfluxdbEmitter() {


        this.client = HttpClientBuilder.create().build();

        log.info("constructing influxdb emitter");
    }

    public void emit(Event event) {
        log.info("emitting from influxdb emitter");
        if (event instanceof ServiceMetricEvent) {
            ServiceMetricEvent metricEvent = (ServiceMetricEvent) event;
            //String metric = metricEvent.getMetric();
            String eventType = metricEvent.getClass().getSimpleName();
            String nodeType = metricEvent.getService().toString();
            String metric = metricEvent.getMetric().toString();
            String value = metricEvent.getValue().toString();
            long time = Instant.now().toEpochMilli() * 1000000;
            //String influxdbMessage = StringUtils.safeFormat("%s service=%s,metric=%s,value=%s %d",influxdbMeasurement,nodeType,metric,value,time);
            String influxdbMessage = "Metric,service=\"" + nodeType + "\",eventType=\"" + eventType + "\",metric=\"" + metric + "\" value=" + value + " " + time;
            log.info(influxdbMessage);

            HttpPost post = new HttpPost("http://localhost:8086/write?db=influxdb");

//            List<NameValuePair> props = new ArrayList<NameValuePair>();
//            props.add(new BasicNameValuePair("service", nodeType));
//            props.add(new BasicNameValuePair("metric", nodeType));
//            props.add(new BasicNameValuePair("value", nodeType));
//            props.add(new BasicNameValuePair("service", nodeType));

            post.setEntity(new StringEntity(influxdbMessage, ContentType.DEFAULT_TEXT));


            post.setHeader("Content-Type","application/x-www-form-urlencoded");


            try {
                client.execute(post);
            } catch(IOException ex) {
                log.info("request failed", ex.getMessage());
            }


            /*try {
                HttpResponse jsonResponse = Unirest.post("http://localhost:8086/write?db=influxdb")
                        .field("data-binary", influxdbMessage)
                        .asString();
                log.info(jsonResponse.getStatusText());
            } catch (UnirestException ex) {
                log.info(ex.toString());
            }*/

        }
    }

    public void start() {
        log.info("starting influxdb emitter");
    }

    public void flush() throws IOException {
        log.info("flushing influxdb emitter");
    }

    public void close() throws IOException {
        log.info("closing influxdb emitter");
    }

}
