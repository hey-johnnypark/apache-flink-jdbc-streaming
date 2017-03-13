import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;

/**
 * Created by johnnypark on 13/03/2017.
 */
public class MetricsMapper<PEEK> extends RichMapFunction<PEEK, PEEK> {

    private Meter meter;
    private Counter counter;

    @Override
    public void open(Configuration config) {
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("my-counter");
        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("throughout", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    }

    @Override
    public PEEK map(PEEK o) throws Exception {
        this.counter.inc();
        this.meter.markEvent();
        return o;
    }
}
