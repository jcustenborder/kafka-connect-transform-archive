/**
 * Copyright © 2018 Jordan Moore (moore.jordan@outlook.com)
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
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;

@Description("The Archive transformation is used to help preserve all of the data for a message as binary when archived at its destination.")
@DocumentationNote("This transform works by copying the key, value, topic, and timestamp to new record where this is all " +
    "contained in the value of the message. This will allow connectors like Confluent's S3 connector to properly archive " +
    "the record as it originated from Kafka")
public class BinaryArchive<R extends ConnectRecord<R>> extends Archive<R> {

  public static Schema getBytesSchema() {
    return SchemaBuilder.bytes().name(ARCHIVE_STORAGE_SCHEMA_NAMESPACE);
  }

  @Override
  public R apply(R r) {
    final Schema schema = getBytesSchema();
    byte[] data;
    try {
      data = new ArchiveData.Builder(r.topic())
              .withTimestamp(r.timestamp())
              .withKey((byte[]) r.key())
              .withValue((byte[]) r.value())
              .build()
              .getBytes();
      return r.newRecord(r.topic(), r.kafkaPartition(), null, null, schema, data, r.timestamp());
    } catch (IOException e) {
      throw new ConnectException("Unable to transform record to byte[]", e);
    }
  }

}
