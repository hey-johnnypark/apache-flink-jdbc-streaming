import io.jp.Protos;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.UUID;
import java.util.stream.IntStream;

/**
 * Created by johnnypark on 13/03/2017.
 */
public class TrackingEventSource implements SourceFunction<Protos.TrackingEvent> {

    private final int numberOfMessages;

    public TrackingEventSource(int numberMessage) {
        this.numberOfMessages = numberMessage;
    }

    @Override
    public void run(SourceContext<Protos.TrackingEvent> sourceContext) throws Exception {
        IntStream
                .range(0, numberOfMessages)
                .forEach((i) -> sourceContext.collect(generateEvent()));
    }

    @Override
    public void cancel() {

    }

    private Protos.TrackingEvent generateEvent() {
        return Protos.TrackingEvent
                .newBuilder()
                .setId(UUID.randomUUID().toString())
                .setMessage("TRACKING")
                .build();
    }


}
