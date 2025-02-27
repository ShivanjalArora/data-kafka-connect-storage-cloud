package io.confluent.connect.s3;

import com.hotstar.utils.StatsDClient;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.CoerceToSegmentPayload;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.hotstar.kafka.connect.transforms.ProtoToPayloadTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.hotstar.constants.Constants.EVENT_NAME_TAG;
import static com.hotstar.kafka.connect.transforms.util.StatsDConstants.STATSD_HOST_DEFAULT;
import static com.hotstar.kafka.connect.transforms.util.StatsDConstants.STATSD_PORT_DEFAULT;

public class S3SinkTaskV2 extends S3SinkTask {
    public static final Logger log = LoggerFactory.getLogger(S3SinkTaskV2.class);
    public static final String KAFKA_PARTITION_TAG = "partition:%s";
    private static final ProtoToPayloadTransform.Value<SinkRecord> protoToPayloadTransform = new ProtoToPayloadTransform.Value<>();
    private static final CoerceToSegmentPayload<SinkRecord> coerceToSegmentPayloadTransform = new CoerceToSegmentPayload.Value<>();

    private static final StatsDClient statsDClient;

    static {
        // This is needed to initialise the statsD client within Proto To Payload transform with default values.
        protoToPayloadTransform.configure(new HashMap<>());
        statsDClient = new StatsDClient.Builder()
                .hostAndPort(STATSD_HOST_DEFAULT, STATSD_PORT_DEFAULT)
                .namespace("connector.custom.metrics.s3.v2")
                .create();

    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {
        ArrayList<SinkRecord> transformedRecords = (ArrayList)transformInParallel(records);
        if(transformedRecords.isEmpty()){
            return;
        }
        SinkRecord firstRecord = transformedRecords.get(0);
        long eventReceivedTimeMs = 0L;
        try {
            Map<String, Object> eventPayload = (Map<String, Object>) firstRecord.value();
            Map<String, Object> properties = (Map<String, Object>) eventPayload.get("properties");
            eventReceivedTimeMs = (Long) properties.get("ts_received_ms");
            // log.warn("ts_received_ms: " + eventReceivedTimeMs);
        } catch (ClassCastException | NullPointerException ex) {
            // Skip the record if it doesn't have the required fields, do nothing
            // log.warn("Skipping the record as it doesn't have the required fields", ex);
        }
        String eventNameTag = String.format(EVENT_NAME_TAG, firstRecord.topic());
        String partitionTag = String.format(KAFKA_PARTITION_TAG, firstRecord.kafkaPartition().toString());
        long putStartTime = System.nanoTime();
        super.put(transformedRecords);
        statsDClient.timing("put.time", System.nanoTime()-putStartTime, eventNameTag);
        statsDClient.gauge("batch.ts_received_ms", eventReceivedTimeMs, eventNameTag, partitionTag);
    }

    private Collection<SinkRecord> transformInParallel(Collection<SinkRecord> sinkRecords) {
        int batchSize = sinkRecords.size();
        if (sinkRecords.isEmpty()){
            return sinkRecords;
        }
        Iterator<SinkRecord> iterator = sinkRecords.iterator();
        String eventNameTag = String.format(EVENT_NAME_TAG, iterator.next().topic());
        statsDClient.gauge("batch.size",batchSize,eventNameTag);
        long startTransformTime = System.nanoTime();
        List<SinkRecord> collect = sinkRecords.parallelStream().map(x -> transformProtoToEventPayload(x)).collect(Collectors.toList());
        statsDClient.timing("transform.time",System.nanoTime()-startTransformTime, eventNameTag);
        return collect;
    }

    private SinkRecord transformProtoToEventPayload(SinkRecord record){
            SinkRecord intermediateRecord = protoToPayloadTransform.apply(record);
            return coerceToSegmentPayloadTransform.apply(intermediateRecord);
    }
}
