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
package io.druid.data.input.parquet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.AvroStreamInputRowParser;
import io.druid.data.input.InputRow;
import io.druid.data.input.avro.record.GenericRecordRowConverter;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import org.apache.avro.generic.GenericRecord;
import java.util.List;

public class ParquetHadoopInputRowParser implements InputRowParser<GenericRecord>
{
  private final ParseSpec parseSpec;
  private final boolean binaryAsString;
  private final TimestampSpec timestampSpec;
  private final GenericRecordRowConverter recordConverter;


  @JsonCreator
  public ParquetHadoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("binaryAsString") Boolean binaryAsString
  )
  {
    this.parseSpec = parseSpec;
    this.timestampSpec = parseSpec.getTimestampSpec();
    this.binaryAsString = binaryAsString == null ? false : binaryAsString;
    this.recordConverter = GenericRecordRowConverter.fromParseSpec(
        this.parseSpec,
        false,
        this.binaryAsString
    );
  }

  /**
   * imitate avro extension {@link AvroStreamInputRowParser#parseGenericRecord(GenericRecord, ParseSpec, List, boolean, boolean)}
   */
  @Override
  public InputRow parse(GenericRecord record)
  {
    return this.recordConverter.convert(record);
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ParquetHadoopInputRowParser(parseSpec, binaryAsString);
  }
}
