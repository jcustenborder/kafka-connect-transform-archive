/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.archive;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;

@Description("The Archive transformation is used to help preserve all of the data for a message when archived to S3.")
@DocumentationNote("This transform works by copying the key, value, topic, and timestamp to new record where this is all " +
    "contained in the value of the message. This will allow connectors like Confluent's S3 connector to properly archive " +
    "the record.")
public class Archive<R extends ConnectRecord<R>> implements Transformation<R> {
  @Override
  public R apply(R r) {
    if (r.valueSchema() == null) {
      return applySchemaless(r);
    } else {
      return applyWithSchema(r);
    }
  }

  private R applyWithSchema(R r) {
    final Schema schema = SchemaBuilder.struct()
        .name("com.github.jcustenborder.kafka.connect.archive.Storage")
        .field("key", r.keySchema())
        .field("value", r.valueSchema())
        .field("topic", Schema.STRING_SCHEMA)
        .field("timestamp", Schema.INT64_SCHEMA);
    Struct value = new Struct(schema)
        .put("key", r.key())
        .put("value", r.value())
        .put("topic", r.topic())
        .put("timestamp", r.timestamp());
    return r.newRecord(r.topic(), r.kafkaPartition(), null, null, schema, value, r.timestamp());
  }

  @SuppressWarnings("unchecked")
  private R applySchemaless(R r) {

    final Map<String, Object> archiveValue = new HashMap<>();

    final Map<String, Object> value = (Map<String, Object>) r.value();

    archiveValue.put("key", r.key());
    archiveValue.put("value", value);
    archiveValue.put("topic", r.topic());
    archiveValue.put("timestamp", r.timestamp());

    return r.newRecord(r.topic(), r.kafkaPartition(), null, null, null, archiveValue, r.timestamp());
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
