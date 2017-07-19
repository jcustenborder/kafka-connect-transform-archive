/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.archive;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

@Description("The Archive transformation is used to help preserve all of the data for a message when archived to S3.")
public class Archive<R extends ConnectRecord<R>> implements Transformation<R> {

  static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
      .name("com.github.jcustenborder.kafka.connect.archive.Storage")
      .field("topic", Schema.OPTIONAL_STRING_SCHEMA)
      .field("key", Schema.OPTIONAL_BYTES_SCHEMA)
      .field("value", Schema.OPTIONAL_BYTES_SCHEMA)
      .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
      .build();


  @Override
  public R apply(R r) {
    final Struct value = new Struct(VALUE_SCHEMA)
        .put("topic", r.topic())
        .put("key", r.key())
        .put("value", r.value())
        .put("timestamp", r.timestamp());
    return r.newRecord(r.topic(), r.kafkaPartition(), Schema.OPTIONAL_BYTES_SCHEMA, r.key(), VALUE_SCHEMA, value, r.timestamp());
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
