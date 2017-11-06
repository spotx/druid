package io.druid.emitter.influxdb;

import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.service.ServiceEventBuilder;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
public class InfluxdbEmitterTest {

    private ServiceMetricEvent event;

    @Before
    public void setUp()
    {
        DateTime date = new DateTime(2017,10,30,10,00); // 10:00am on 30/10/2017 = 1509357600000000000 in epoch nanoseconds
        String metric = "metric/test/value";
        Number value = 1234;
        ImmutableMap<String, String> serviceDims = ImmutableMap.of("service", "druid/historical", "host", "localhost", "version", "0.10.0");
        ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
        ServiceEventBuilder eventBuilder = builder.build(date, metric, value);
        event = (ServiceMetricEvent) eventBuilder.build(serviceDims);
    }

    @Test
    public void testTransformForInfluxWithEmptyConfig() {
        InfluxdbEmitterConfig config = new InfluxdbEmitterConfig(
                "localhost",
                8086,
                "dbname",
                10000,
                "Metric",
                null,
                "value",
                15000,
                30000,
                "adam",
                "password"
        );
        InfluxdbEmitter influxdbEmitter = new InfluxdbEmitter(config);
        String expected = "Metric value=1234 1509357600000000000" + "\n";
        String actual = influxdbEmitter.transformForInflux(event);
        Assert.assertEquals(expected,actual);
    }

    /**
     * Test setting measurement in config to be the service type of the event.
     * Measurement is like a table in influxdb
     */
    @Test
    public void testTransformForInfluxWithConfigMeasurement() {
        InfluxdbEmitterConfig config = new InfluxdbEmitterConfig(
                "localhost",
                8086,
                "dbname",
                10000,
                "service",
                null,
                "value",
                15000,
                30000,
                "adam",
                "password"
        );
        InfluxdbEmitter influxdbEmitter = new InfluxdbEmitter(config);
        String expected = "druid/historical value=1234 1509357600000000000" + "\n"; // druid/historical is the service and this was set as the measurement
        String actual = influxdbEmitter.transformForInflux(event);
        Assert.assertEquals(expected,actual);
    }

    @Test
    public void testTransformForInfluxWithTagsAndFields() {
        InfluxdbEmitterConfig config = new InfluxdbEmitterConfig(
                "localhost",
                8086,
                "dbname",
                10000,
                "Metric",
                "feed,eventType,host",
                "metric,service",
                15000,
                30000,
                "adam",
                "password"
        );
        InfluxdbEmitter influxdbEmitter = new InfluxdbEmitter(config);
        String expected = "Metric,feed=metrics,eventType=ServiceMetricEvent,host=localhost metric=metric/test/value,service=druid/historical 1509357600000000000" + "\n";
        String actual = influxdbEmitter.transformForInflux(event);
        Assert.assertEquals(expected,actual);
    }


}
