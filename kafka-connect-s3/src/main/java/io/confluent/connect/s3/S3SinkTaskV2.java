package io.confluent.connect.s3;

import com.hotstar.utils.StatsDClient;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.CoerceToSegmentPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.hotstar.kafka.connect.transforms.ProtoToPayloadTransform;

import static com.hotstar.constants.Constants.EVENT_NAME_TAG;
import static com.hotstar.kafka.connect.transforms.util.StatsDConstants.STATSD_HOST_DEFAULT;
import static com.hotstar.kafka.connect.transforms.util.StatsDConstants.STATSD_PORT_DEFAULT;

public class S3SinkTaskV2 extends S3SinkTask {
    private ExecutorService executorService;
    public static final long THREAD_KEEP_ALIVE_TIME_SECONDS = 60L;
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SinkTaskV2.class);

    private static final ProtoToPayloadTransform.Value<SinkRecord> protoToPayloadTransform = new ProtoToPayloadTransform.Value<>();
    private static final CoerceToSegmentPayload<SinkRecord> coerceToSegmentPayloadTransform = new CoerceToSegmentPayload.Value<>();

    private static final StatsDClient statsDClient;

    static {
        // This is needed to initialise the statsD client within Proto To Payload transform with default values.
        protoToPayloadTransform.configure(new HashMap<>());
        statsDClient = new StatsDClient.Builder()
                .hostAndPort(STATSD_HOST_DEFAULT, STATSD_PORT_DEFAULT)
                .namespace("s3connector.v2")
                .create();
    }


    @Override
    public void start(Map<String, String> props) {
        super.start(props);
        int transformThreadPoolMaxSize = Integer.parseInt(props.get("transform.threads.max"));
        this.executorService = new ThreadPoolExecutor(1, transformThreadPoolMaxSize,
                THREAD_KEEP_ALIVE_TIME_SECONDS,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {
        ArrayList<SinkRecord> transformedRecords = (ArrayList)transformInParallel(records);
        if(transformedRecords.size() == 0 ){
            return;
        }
        String eventNameTag = String.format(EVENT_NAME_TAG, transformedRecords.get(0).topic());
        long putStartTime = System.nanoTime();
        super.put(transformedRecords);
        statsDClient.timing("s3.put.time", System.nanoTime()-putStartTime, eventNameTag);
    }

    private Collection<SinkRecord> transformInParallel(Collection<SinkRecord> sinkRecords) {
        int batchSize = sinkRecords.size();
        if (batchSize == 0 ){
            return sinkRecords;
        }
        long startTransformTime = System.nanoTime();
        SinkRecord[] kafkaRecordArray = sinkRecords.toArray(new SinkRecord[0]);
        String eventNameTag = String.format(EVENT_NAME_TAG, kafkaRecordArray[0].topic());
        statsDClient.gauge("batch.size",batchSize,eventNameTag);
        List<Future<SinkRecord>> futureList = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            int recordNumber = i;
            futureList.add(executorService.submit((Callable) () -> transformProtoToEventPayload(kafkaRecordArray[recordNumber])));
        }
        List<SinkRecord> transformedRecords = new ArrayList<>(batchSize);
        futureList.stream().forEach(future -> {
            try {
                SinkRecord transformedRecord = future.get();
                transformedRecords.add(transformedRecord);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        statsDClient.timing("transform.time",System.nanoTime()-startTransformTime, eventNameTag);
        return transformedRecords;
    }

    private SinkRecord transformProtoToEventPayload(SinkRecord record){
            SinkRecord intermediateRecord = protoToPayloadTransform.apply(record);
            return coerceToSegmentPayloadTransform.apply(intermediateRecord);
    }
}
