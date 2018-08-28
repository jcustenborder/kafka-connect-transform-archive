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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public class ArchiveData {
  private byte[] key;
  private byte[] value;
  private String topic;
  private long timestamp = -1L;

  private ArchiveData() {}

  public ArchiveData(byte[] data) throws IOException {
    if (data == null) {
      return;
    }
    try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
         DataInputStream dis = new DataInputStream(bais)) {
      int offset = 0;

      // TopicName: length + utf8 bytes
      int topicBytesLen = dis.readInt();
      offset += Integer.BYTES;
      if (topicBytesLen > 0) {
        this.topic = new String(Arrays.copyOfRange(data, offset, offset + topicBytesLen), StandardCharsets.UTF_8);
        offset += dis.read(data, offset, topicBytesLen);
      }

      // Timestamp
      this.timestamp = dis.readLong();
      offset += Long.BYTES;

      // key as byte[]
      int keySize = dis.readInt();
      offset += Integer.BYTES;
      if (keySize > 0) {
        this.key = Arrays.copyOfRange(data, offset, offset + keySize);
        offset += dis.read(data, offset, keySize);
      }

      // value as byte[]
      int valueSize = dis.readInt();
      offset += Integer.BYTES;
      if (valueSize > 0) {
        this.value = Arrays.copyOfRange(data, offset, offset + valueSize);
        offset += dis.read(data, offset, valueSize);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArchiveData that = (ArchiveData) o;
    return Arrays.equals(key, that.key) &&
            Arrays.equals(value, that.value) &&
            Objects.equals(topic, that.topic) &&
            Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(topic, timestamp);
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  public static class Builder {

    private ArchiveData model;

    private Builder() {}

    public Builder(String topicName) {
      this.model = new ArchiveData();
      this.model.topic = topicName;
    }

    public Builder withKey(byte[] key) {
      model.key = key;
      return this;
    }

    public Builder withValue(byte[] value) {
      model.value = value;
      return this;
    }

    public Builder withTimestamp(long timestamp) {
      model.timestamp = timestamp;
      return this;
    }

    public ArchiveData build() {
      if (model.topic == null) {
        throw new RuntimeException("ArchiveData must have a topic name");
      }
      return model;
    }
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  public String getTopic() {
    return topic;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "ArchiveData{" +
            "topic=" + topic +
            ", timestamp=" + timestamp +
            ", key='" + Arrays.toString(key) + '\'' +
            ", value=" + Arrays.toString(value) +
            '}';
  }

  public byte[] getBytes() throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream dos = new DataOutputStream(baos)) {

      // TopicName: int + utf8 string
      byte[] topicBytes = null;
      int topicByteLen = 0;
      if (this.topic != null) {
        topicBytes = this.topic.getBytes(StandardCharsets.UTF_8);
        topicByteLen = topicBytes.length;
      }
      dos.writeInt(topicByteLen);
      if (topicBytes != null) {
        dos.write(topicBytes);
      }

      // Timestamp: long
      dos.writeLong(this.timestamp);

      // key as byte[]
      dos.writeInt(this.key == null ? 0 : this.key.length);
      if (this.key != null) {
        dos.write(this.key);
      }
      // value as byte[]
      dos.writeInt(this.value == null ? 0 : this.value.length);
      if (this.value != null) {
        dos.write(this.value);
      }

      dos.flush();
      return baos.toByteArray();
    }
  }
}
