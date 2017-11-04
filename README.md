# Introduction

# Transformations

## Archive

The Archive transformation is used to help preserve all of the data for a message when archived to S3.

### Configuration

| Name | Type | Importance | Default Value | Validator | Documentation|
| ---- | ---- | ---------- | ------------- | --------- | -------------|


#### Standalone Example

```properties
transforms=Archive
transforms.Archive.type=com.github.jcustenborder.kafka.connect.archive.Archive
# The following values must be configured.
```

#### Distributed Example

```json
{
    "name": "connector1",
    "config": {
        "connector.class": "com.github.jcustenborder.kafka.connect.archive.Archive",
        "transforms": "Archive",
        "transforms.Archive.type": "com.github.jcustenborder.kafka.connect.archive.Archive",
    }
}
```

