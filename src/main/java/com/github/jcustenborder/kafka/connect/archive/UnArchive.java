/**
 * Copyright © 2022 Iosif Nicolae (iosif@bringes.io)
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
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

import static org.apache.commons.lang3.SerializationUtils.deserialize;


@Description("The UnArchive transformation is used to unarchive data from S3 into the original format.")
@DocumentationNote("This transform works by copying the key, value, topic, and timestamp to new record where this is all " +
    "contained in the value of the message. This will allow connectors like Confluent's S3 connector to properly unarchive " +
    "the record.")
public class UnArchive<R extends ConnectRecord<R>> implements Transformation<R> {
  @Override
  public R apply(R r) {
    return applySchemaless(r);
  }
  @SuppressWarnings("unchecked")
  private R applySchemaless(R r) {
    final Map<String, Object> value = (Map<String, Object>) r.value();
    return r.newRecord(
        r.topic(),
        value.get("partition") != null ? Integer.parseInt(value.get("partition").toString()) : null,
        null,
        deserialize((byte[]) value.get("key")),
        null,
        value.get("value"),
        Long.parseLong(value.get("timestamp").toString())
    );
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
