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
        String metric = "metric/te/st/value";
        Number value = 1234;
        ImmutableMap<String, String> serviceDims = ImmutableMap.of("service", "druid/historical", "host", "localhost", "version", "0.10.0");
        ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
        ServiceEventBuilder eventBuilder = builder.build(date, metric, value);
        event = (ServiceMetricEvent) eventBuilder.build(serviceDims);
    }

    @Test
    public void testTransformForInfluxWithLongMetric() {
        InfluxdbEmitterConfig config = new InfluxdbEmitterConfig(
                "localhost",
                8086,
                "dbname",
                10000,
                15000,
                30000,
                "adam",
                "password"
        );
        InfluxdbEmitter influxdbEmitter = new InfluxdbEmitter(config);
        String expected = "druid_metric,service=druid/historical,metric=druid_te_st,hostname=localhost druid_value=1234 1509357600000000000" + "\n";
        String actual = influxdbEmitter.transformForInfluxSystems(event);
        Assert.assertEquals(expected,actual);
    }

    @Test
    public void testTransformForInfluxWithShortMetric() {
        DateTime date = new DateTime(2017,10,30,10,00); // 10:00am on 30/10/2017 = 1509357600000000000 in epoch nanoseconds
        String metric = "metric/time";
        Number value = 1234;
        ImmutableMap<String, String> serviceDims = ImmutableMap.of("service", "druid/historical", "host", "localhost", "version", "0.10.0");
        ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
        ServiceEventBuilder eventBuilder = builder.build(date, metric, value);
        ServiceMetricEvent event = (ServiceMetricEvent) eventBuilder.build(serviceDims);
        InfluxdbEmitterConfig config = new InfluxdbEmitterConfig(
                "localhost",
                8086,
                "dbname",
                10000,
                15000,
                30000,
                "adam",
                "password"
        );
        InfluxdbEmitter influxdbEmitter = new InfluxdbEmitter(config);
        String expected = "druid_metric,service=druid/historical,hostname=localhost druid_time=1234 1509357600000000000" + "\n";
        String actual = influxdbEmitter.transformForInfluxSystems(event);
        Assert.assertEquals(expected,actual);
    }



}
