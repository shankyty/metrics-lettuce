package io.shashanktyagi.metrics.lettuce;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.lambdaworks.redis.metrics.CommandMetrics;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static javax.swing.UIManager.put;

/**
 * Latency metrics for commands. This class provides the count, time unit and firstResponse/completion latencies.
 *
 * @author Shashank Tyagi
 * @since 0.1
 */
public class CodeHaleCommandMetrics extends CommandMetrics {
  private Timer timer;
  private TimeUnit timeUnit;

  public CodeHaleCommandMetrics(Timer firstResponse, Timer completion, TimeUnit timeUnit) {
    super(firstResponse.getCount(),
        timeUnit,
        new CodeHaleCommandLatency(firstResponse.getSnapshot(), timeUnit),
        new CodeHaleCommandLatency(completion.getSnapshot(), timeUnit));
  }


  public static class CodeHaleCommandLatency extends CommandLatency {


    public CodeHaleCommandLatency(Snapshot snapshot, final TimeUnit timeUnit) {
      super(snapshot.getMin(),
          snapshot.getMax(),
          ImmutableMap.of(
              75.0d, timeUnit.convert(Double.doubleToLongBits(snapshot.get75thPercentile()), TimeUnit.NANOSECONDS),
              95.0d, timeUnit.convert(Double.doubleToLongBits(snapshot.get95thPercentile()), TimeUnit.NANOSECONDS),
              98.0d, timeUnit.convert(Double.doubleToLongBits(snapshot.get98thPercentile()), TimeUnit.NANOSECONDS),
              99.0d, timeUnit.convert(Double.doubleToLongBits(snapshot.get99thPercentile()), TimeUnit.NANOSECONDS),
              99.9d, timeUnit.convert(Double.doubleToLongBits(snapshot.get999thPercentile()), TimeUnit.NANOSECONDS)
          );
    }
  }
}
