package com.github.jcustenborder.kafka.connect.archive.serialization;

import com.github.jcustenborder.kafka.connect.archive.Archive;
import com.github.jcustenborder.kafka.connect.archive.ArchiveData;
import com.github.jcustenborder.kafka.connect.archive.BinaryArchive;
import com.github.jcustenborder.kafka.connect.archive.converters.ArchiveByteArrayConverter;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;

public class SerdeTest {

    private final long timestamp = 1528243200L;
    private final String topic = "archival";
    private final String key = "some-key hello world";
    private final String value = "some-value";

    private ArchiveSerializer serializer;
    private ArchiveDeserializer deserializer;

    @BeforeEach
    void setUp() {
        serializer = new ArchiveSerializer();
        deserializer = new ArchiveDeserializer();
    }

    @Test
    void testSerde() {

        ArchiveData a = new ArchiveData.Builder(topic)
                .withTimestamp(timestamp)
                .withKey(Utils.utf8(key))
                .withValue(Utils.utf8(value))
                .build();

        byte[] data = serializer.serialize(topic, a);

        ArchiveData a2 = deserializer.deserialize(topic, data);

        Assert.assertThat(a, is(a2));

    }

    @Test
    void testSerdeNullKey() {

        ArchiveData a = new ArchiveData.Builder(topic)
                .withTimestamp(timestamp)
                .withValue(Utils.utf8(value))
                .build();

        byte[] data = serializer.serialize(topic, a);

        ArchiveData a2 = deserializer.deserialize(topic, data);

        Assert.assertThat(a, is(a2));

    }

    @Test
    void testSerdeNullValue() {

        ArchiveData a = new ArchiveData.Builder(topic)
                .withTimestamp(timestamp)
                .withKey(Utils.utf8(key))
                .build();

        byte[] data = serializer.serialize(topic, a);

        ArchiveData a2 = deserializer.deserialize(topic, data);

        Assert.assertThat(a, is(a2));

    }

    @Test
    void testSerdeNullKeyNullValue() {

        ArchiveData a = new ArchiveData.Builder(topic)
                .withTimestamp(timestamp)
                .build();

        byte[] data = serializer.serialize(topic, a);

        ArchiveData a2 = deserializer.deserialize(topic, data);

        Assert.assertThat(a, is(a2));

    }

    @Test
    void testSerdeDefaults() {

        ArchiveData a = new ArchiveData.Builder(topic).build();

        byte[] data = serializer.serialize(topic, a);

        ArchiveData a2 = deserializer.deserialize(topic, data);

        Assert.assertThat(a, is(a2));
    }

    @Test
    void testConverter() {
        ArchiveByteArrayConverter c = new ArchiveByteArrayConverter();
        Map<String, String> conf = new HashMap<>();

        ArchiveData a = new ArchiveData.Builder(topic)
                .withTimestamp(timestamp)
                .withKey(Utils.utf8(key))
                .withValue(Utils.utf8(value))
                .build();

        conf.put("archive.format", "struct");
        c.configure(conf, false);
        Schema s = Archive.getStructSchema(Schema.OPTIONAL_BYTES_SCHEMA, Schema.OPTIONAL_BYTES_SCHEMA);
        byte[] data = c.fromConnectData(topic, s, a);
        SchemaAndValue sv = c.toConnectData(topic, data);
        Assert.assertThat(s.name(), is(Archive.ARCHIVE_STORAGE_SCHEMA_NAMESPACE));
        Assert.assertThat(a, is(sv.value()));

        conf.put("archive.format", "binary");
        c.configure(conf, false);
        s = BinaryArchive.getBytesSchema();
        data = c.fromConnectData(topic, s, a);
        sv = c.toConnectData(topic, data);
        Assert.assertThat(s.name(), is(Archive.ARCHIVE_STORAGE_SCHEMA_NAMESPACE));
        Assert.assertThat(a, is(sv.value()));

    }
}
