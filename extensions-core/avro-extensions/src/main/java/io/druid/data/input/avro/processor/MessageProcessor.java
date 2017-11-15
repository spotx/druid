package io.druid.data.input.avro.processor;

import io.druid.data.input.avro.processor.mappers.RegExMapper;

import java.util.Map;

public class MessageProcessor
{
  public static Map process(Map<String, Object> map) {

    //Temporary hack for referrer domain stripping
    //until we have a proper ETL in Druid.
    if( map.containsKey("referrer_domain") ) {

      RegExMapper referrerMapper = new RegExMapper(
          "referrer_domain",
          ".*//([^/]+)/"
      );




      map = referrerMapper.map(map);
    }

    return map;
  }
}
