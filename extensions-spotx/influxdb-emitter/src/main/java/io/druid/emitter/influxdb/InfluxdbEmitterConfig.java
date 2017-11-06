package io.druid.emitter.influxdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;
import io.druid.java.util.common.logger.Logger;

public class InfluxdbEmitterConfig {

    private final static int DEFAULT_PORT = 8086;
    private final static String DEFAULT_FIELD = "value";
    private final static List<String> ACCEPTED_VALUES = Arrays.asList("metric,service,value,feed,host,eventType".split(","));
    private final static int DEFAULT_QUEUE_SIZE = Integer.MAX_VALUE;
    private final static int DEFAULT_FLUSH_PERIOD = 60000; // milliseconds

    @JsonProperty
    final private String hostname;
    @JsonProperty
    final private Integer port;
    @JsonProperty
    final private String databaseName;
    @JsonProperty
    final private Integer maxQueueSize;
    @JsonProperty
    final private String measurement;
    @JsonProperty
    final private String tags;
    @JsonProperty
    final private String fields;
    @JsonProperty
    final private Integer flushPeriod;
    @JsonProperty
    final private Integer flushDelay;
    @JsonProperty
    final private String influxdbUserName;
    @JsonProperty
    final private String influxdbPassword;


    private static Logger log = new Logger(InfluxdbEmitterConfig.class);

    @JsonCreator
    public InfluxdbEmitterConfig(@JsonProperty("hostname") String hostname,
                                 @JsonProperty("port") Integer port,
                                 @JsonProperty("databaseName") String databaseName,
                                 @JsonProperty("maxQueueSize") Integer maxQueueSize,
                                 @JsonProperty("measurement") String measurement,
                                 @JsonProperty("tags") String tags,
                                 @JsonProperty("fields") String fields,
                                 @JsonProperty("flushPeriod") Integer flushPeriod,
                                 @JsonProperty("flushDelay") Integer flushDelay,
                                 @JsonProperty("influxdbUserName") String influxdbUserName,
                                 @JsonProperty("influxdbPassword") String influxdbPassword)
    {
        this.hostname = Preconditions.checkNotNull(hostname, "hostname can not be null");
        this.port = port == null ? DEFAULT_PORT : port;
        this.databaseName = Preconditions.checkNotNull(databaseName, "databaseName can not be null");
        this.maxQueueSize = maxQueueSize == null ? DEFAULT_QUEUE_SIZE : maxQueueSize;
        this.measurement = Preconditions.checkNotNull(measurement, "measurement can not be null");
        this.tags = tags == null ?  "" : (ACCEPTED_VALUES.containsAll(Arrays.asList(tags.split(","))) || Arrays.asList(tags.split(",")).size() == 0) ? tags : "";
        this.fields = (ACCEPTED_VALUES.containsAll(Arrays.asList(fields.split(","))) && Arrays.asList(fields.split(",")).size() != 0) ? fields : DEFAULT_FIELD;
        this.flushPeriod = flushPeriod == null ? DEFAULT_FLUSH_PERIOD : flushPeriod;
        this.flushDelay = flushDelay == null ? DEFAULT_FLUSH_PERIOD : flushDelay;
        this.influxdbUserName = Preconditions.checkNotNull(influxdbUserName, "influxdbUserName can not be null");
        this.influxdbPassword = Preconditions.checkNotNull(influxdbPassword, "influxdbPassword can not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InfluxdbEmitterConfig)) {
            return false;
        }

        InfluxdbEmitterConfig that = (InfluxdbEmitterConfig) o;

        if (getPort() != that.getPort()) {
            return false;
        }
        if (!getHostname().equals(that.getHostname())) {
            return false;
        }
        if (!getDatabaseName().equals(that.getDatabaseName())) {
            return false;
        }
        if (!getFields().equals(that.getFields())){
            return false;
        }
        if (!getMeasurement().equals(that.getMeasurement())){
            return false;
        }
        if (!getTags().equals(that.getTags())){
            return false;
        }
        if (getFlushPeriod() != that.getFlushPeriod()){
            return false;
        }
        if (getMaxQueueSize() != that.getMaxQueueSize()) {
            return false;
        }
        if (getFlushDelay() != that.getFlushDelay()) {
            return false;
        }
        if (!getInfluxdbUserName().equals(that.getInfluxdbUserName())) {
            return false;
        }
        if (!getInfluxdbPassword().equals(that.getInfluxdbPassword())) {
            return false;
        }
        return true;

    }

    @Override
    public int hashCode() {
        int result = getHostname().hashCode();
        result = 31 * result + getPort();
        result = 31 * result + getDatabaseName().hashCode();
        result = 31 * result + getMeasurement().hashCode();
        result = 31 * result + getTags().hashCode();
        result = 31 * result + getFields().hashCode();
        result = 31 * result + getFlushPeriod();
        result = 31 * result + getMaxQueueSize();
        result = 31 * result + getFlushDelay();
        result = 31 * result + getInfluxdbUserName().hashCode();
        result = 31 * result + getInfluxdbPassword().hashCode();
        return result;
    }

    @JsonProperty
    public String getHostname() {
        return hostname;
    }

    @JsonProperty
    public int getPort() {
        return port;
    }

    @JsonProperty
    public String getDatabaseName() {
        return databaseName;
    }

    @JsonProperty
    public String getMeasurement() {
        return measurement;
    }

    @JsonProperty
    public List<String> getTags() {
        if(tags.length() == 0) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(tags.split(","));
        }
    }

    @JsonProperty
    public List<String> getFields() {
        return Arrays.asList(fields.split(","));
    }

    @JsonProperty
    public int getFlushPeriod() {
        return flushPeriod;
    }

    @JsonProperty
    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    @JsonProperty
    public int getFlushDelay() {
        return flushDelay;
    }

    @JsonProperty
    public String getInfluxdbUserName() {
        return influxdbUserName;
    }

    @JsonProperty
    public String getInfluxdbPassword() { return influxdbPassword; }

}
