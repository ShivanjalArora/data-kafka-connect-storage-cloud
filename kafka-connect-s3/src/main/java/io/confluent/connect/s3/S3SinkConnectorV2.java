package io.confluent.connect.s3;

import org.apache.kafka.connect.connector.Task;


public class S3SinkConnectorV2 extends S3SinkConnector {
    @Override
    public Class<? extends Task> taskClass() {
        return S3SinkTaskV2.class;
    }
    public S3SinkConnectorV2() {
        // no-arg constructor required by Connect framework.

    }

}
