/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.data.input.avro.processor.mappers;


import io.druid.data.input.avro.processor.mappers.IMapper;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Map;

/**
 *
 */
public class RegExMapper implements IMapper
{
  private final String  field;
  private final Pattern pattern;

  public RegExMapper(String field, String regEx) {
    this.pattern = Pattern.compile(regEx);
    this.field = field;
  }


  @Override
  public boolean canMap(Map<String,Object> map) {
    return map.containsKey(this.field);
  }

  @Override
  public Map<String,Object> map(Map<String,Object> map)
  {
    Object value = map.get(field);

    if(value != null) {

      Matcher m = pattern.matcher(value.toString());

      if( m.find() ) {

        map.put(field, m.group(0));
      }

    }

    return map;
  }
}