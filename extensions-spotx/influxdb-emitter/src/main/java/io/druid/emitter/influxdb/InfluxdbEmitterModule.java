package io.druid.emitter.influxdb;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import io.druid.guice.JsonConfigProvider;
import io.druid.initialization.DruidModule;
import java.util.Collections;
import java.util.List;
import com.metamx.common.logger.Logger;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.druid.guice.ManageLifecycle;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.emitter.core.Emitter;

public class InfluxdbEmitterModule implements DruidModule{

    private static final String EMITTER_TYPE = "influxdb";
    private static final Logger log = new Logger(InfluxdbEmitterModule.class);

    public List<? extends Module> getJacksonModules() {
        return Collections.EMPTY_LIST;
    }

    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "druid.emitter." + EMITTER_TYPE, InfluxdbEmitterConfig.class);
    }

    @Provides
    @ManageLifecycle
    @Named(EMITTER_TYPE)
    public Emitter getEmitter(InfluxdbEmitterConfig influxdbEmitterConfig, ObjectMapper mapper)
    {
        return new InfluxdbEmitter(influxdbEmitterConfig);
    }
}
