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
package com.github.jcustenborder.kafka.connect.archive.serialization;

import com.github.jcustenborder.kafka.connect.archive.ArchiveData;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class ArchiveSerializer implements Serializer<ArchiveData> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public byte[] serialize(String topic, ArchiveData data) {
    try {
      return data == null ? null : data.getBytes();
    } catch (IOException e) {
      throw new SerializationException("Unable to serialize: " + data.toString());
    }
  }

  @Override
  public void close() {

  }
}
