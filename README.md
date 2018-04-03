
# Introduction



# Transformations


## Archive

The Archive transformation is used to help preserve all of the data for a message when archived to S3.




### Note

This transform works by copying the key, value, topic, and timestamp to new record where this is all contained in the value of the message. This will allow connectors like Confluent's S3 connector to properly archive the record.


### Configuration


#### Examples

##### Standalone Example

This configuration is used typically along with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=Connector1
connector.class=org.apache.kafka.some.SourceConnector
tasks.max=1
transforms=tran
transforms.tran.type=com.github.jcustenborder.kafka.connect.archive.Archive
```

##### Distributed Example

This configuration is used typically along with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers).
Write the following json to `connector.json`, configure all of the required values, and use the command below to
post the configuration to one the distributed connect worker(s).

```json
{
  "name" : "Connector1",
  "connector.class" : "org.apache.kafka.some.SourceConnector",
  "transforms" : "tran",
  "transforms.tran.type" : "com.github.jcustenborder.kafka.connect.archive.Archive"
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.
```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.
```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```


