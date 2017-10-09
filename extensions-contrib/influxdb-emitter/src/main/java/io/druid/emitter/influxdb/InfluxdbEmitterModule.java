package io.druid.emitter.influxdb;

//not sure which imports will be needed - taken all from AmbariMetricsEmitterModule

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import io.druid.guice.JsonConfigProvider;
import io.druid.initialization.DruidModule;
import java.util.Collections;
import java.util.List;

public class InfluxdbEmitterModule implements DruidModule{

    private static final String EMITTER_TYPE = "influxdb";

    public List<? extends Module> getJacksonModules() {
        return Collections.EMPTY_LIST;
    }

    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "druid.emitter." + EMITTER_TYPE, InfluxdbEmitterConfig.class);
        System.out.println("hello world from emitter class");
    }

    /*@Provides
    @ManageLifecycle
    @Named(EMITTER_TYPE)
    public Emitter getEmitter(InfluxdbEmitterConfig config, ObjectMapper mapper)
    {
        return StatsDEmitter.of(config, mapper);
    }*/

}
