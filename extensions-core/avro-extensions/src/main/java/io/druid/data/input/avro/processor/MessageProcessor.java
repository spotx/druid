package io.druid.data.input.avro.processor;

import io.druid.data.input.avro.processor.mappers.RegExMapper;

import java.util.Map;

public class MessageProcessor
{
  private static RegExMapper referrerMapper = new RegExMapper(
      "referrer_domain",
      "([a-z0-9|-]+\\.)*[a-z0-9|-]+\\.[a-z]+"
  );

  public static Map process(Map<String, Object> map) {

    //Temporary hack for referrer domain stripping
    //until we have a proper ETL in Druid.
    if( referrerMapper.canMap(map) ) {
      map = referrerMapper.map(map);
    }

    return map;
  }
}
