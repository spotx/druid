package io.druid.emitter.influxdb;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class InfluxdbEmitterConfigTest {
    private ObjectMapper mapper = new DefaultObjectMapper();
    private InfluxdbEmitterConfig influxdbEmitterConfig;

    @Before
    public void setUp()
    {
        mapper.setInjectableValues(new InjectableValues.Std().addValue(
                ObjectMapper.class,
                new DefaultObjectMapper()
        ));

        influxdbEmitterConfig = new InfluxdbEmitterConfig(
                "localhost",
                8086,
                "dbname",
                10000,
                "Metric",
                "feed,metric,service",
                "value",
                15000,
                30000,
                "adam",
                "password"
                );
    }

    @Test
    public void testInfluxdbEmitterConfigObjectsAreDifferent() throws IOException {
        InfluxdbEmitterConfig influxdbEmitterConfigComparison = new InfluxdbEmitterConfig(
                "localhost",
                8080,
                "dbname",
                10000,
                "Metric",
                "eventType,host",
                "value",
                15000,
                30000,
                "adam",
                "password"
        );
        Assert.assertNotEquals(influxdbEmitterConfig, influxdbEmitterConfigComparison);
    }

    @Test
    public void testConfigWithEmptyTags() throws IOException {
        InfluxdbEmitterConfig influxdbEmitterConfigEmptyTags = new InfluxdbEmitterConfig(
                "localhost",
                8080,
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
        List<String> expectedTags = Collections.emptyList();
        Assert.assertEquals(influxdbEmitterConfigEmptyTags.getTags(), expectedTags);
    }

    @Test (expected = NullPointerException.class)
    public void testConfigWithNullHostname() throws IOException {
        InfluxdbEmitterConfig influxdbEmitterConfigWithNullHostname = new InfluxdbEmitterConfig(
                null,
                8080,
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
    }

    @Test
    public void testConfigWithNullPort() throws IOException {
        InfluxdbEmitterConfig influxdbEmitterConfigWithNullPort = new InfluxdbEmitterConfig(
                "localhost",
                null,
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
        int expectedPort = 8086;
        Assert.assertEquals(expectedPort, influxdbEmitterConfig.getPort());
    }

    @Test
    public void testConfigWithNotAcceptedFields() throws IOException {
        InfluxdbEmitterConfig influxdbEmitterConfigWithNotAcceptedFields = new InfluxdbEmitterConfig(
                "localhost",
                8086,
                "dbname",
                10000,
                "Metric",
                null,
                "testField",
                15000,
                30000,
                "adam",
                "password"
        );
        Assert.assertEquals(Arrays.asList("value"), influxdbEmitterConfig.getFields());
    }

    @Test
    public void testEqualsMethod() {
        InfluxdbEmitterConfig influxdbEmitterConfigComparison = new InfluxdbEmitterConfig(
                "localhost",
                8086,
                "dbname",
                10000,
                "Metric",
                "feed,metric,service",
                "value",
                15000,
                30000,
                "adam",
                "password"
        );
        Assert.assertTrue(influxdbEmitterConfig.equals(influxdbEmitterConfigComparison));
    }

    @Test
    public void testEqualsMethodWithNotEqualConfigs() {
        InfluxdbEmitterConfig influxdbEmitterConfigComparison = new InfluxdbEmitterConfig(
                "localhost",
                8086,
                "dbname",
                10000,
                "Metric",
                "feed,metric,service",
                "value",
                15000,
                10000,
                "adam",
                "password"
        );
        Assert.assertFalse(influxdbEmitterConfig.equals(influxdbEmitterConfigComparison));
    }

    @Test (expected = NullPointerException.class)
    public void testConfigWithNullInfluxdbUserName() throws IOException {
        InfluxdbEmitterConfig influxdbEmitterConfigWithNullHostname = new InfluxdbEmitterConfig(
                "localhost",
                8086,
                "dbname",
                10000,
                "Metric",
                null,
                "value",
                15000,
                30000,
                null,
                "password"
        );
    }

    @Test (expected = NullPointerException.class)
    public void testConfigWithNullInfluxdbPassword() throws IOException {
        InfluxdbEmitterConfig influxdbEmitterConfigWithNullHostname = new InfluxdbEmitterConfig(
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
                null
        );
    }



}
