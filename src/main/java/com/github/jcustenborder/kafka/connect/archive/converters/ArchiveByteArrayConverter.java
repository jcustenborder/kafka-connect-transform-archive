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
package com.github.jcustenborder.kafka.connect.archive.converters;

import com.github.jcustenborder.kafka.connect.archive.Archive;
import com.github.jcustenborder.kafka.connect.archive.ArchiveData;
import com.github.jcustenborder.kafka.connect.archive.BinaryArchive;
import com.github.jcustenborder.kafka.connect.archive.serialization.ArchiveDeserializer;
import com.github.jcustenborder.kafka.connect.archive.serialization.ArchiveSerializer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ArchiveByteArrayConverter extends ByteArrayConverter {

  private final ArchiveSerializer serializer = new ArchiveSerializer();
  private final ArchiveDeserializer deserializer = new ArchiveDeserializer();

  private String storageFormat;

  private static final List<String> FORMATS = Arrays.asList(
    "struct", "binary"
  );

  public ArchiveByteArrayConverter() {
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);

    String format = String.valueOf(configs.get("archive.format"));
    if (FORMATS.contains(format)) {
      this.storageFormat = format;
    } else {
      throw new ConnectException("Invalid archive.format: " + format);
    }

    this.serializer.configure(configs, isKey);
    this.deserializer.configure(configs, isKey);
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    if (schema == null) {
      throw new DataException("Schema is not defined. Must be STRUCT or BYTES");
    }
    if (!schema.name().equals(Archive.ARCHIVE_STORAGE_SCHEMA_NAMESPACE)) {
      throw new DataException(String.format(
              "Invalid schema namespace for %s: %s",
              ArchiveByteArrayConverter.class.getSimpleName(),
              schema.name()));
    }

    try {
      return serializer.serialize(topic, value == null ? null : (ArchiveData) value);
    } catch (SerializationException e) {
      throw new DataException("Failed to serialize to an ArchiveData: ", e);
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    Schema schema = this.storageFormat.equals("struct") ?
            Archive.getStructSchema(Schema.OPTIONAL_BYTES_SCHEMA, Schema.OPTIONAL_BYTES_SCHEMA)
            : BinaryArchive.getBytesSchema();
    try {
      ArchiveData connectValue = deserializer.deserialize(topic, value);
      return new SchemaAndValue(schema, connectValue);
    } catch (SerializationException e) {
      throw new DataException("Failed to deserialize ArchiveData: ", e);
    }
  }
}
