package io.shashanktyagi.metrics.lettuce;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.metrics.*;
import com.lambdaworks.redis.protocol.ProtocolKeyword;
import io.netty.channel.local.LocalAddress;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@link CodehaleMetricCollector} for command latencies. Command latencies are collected per connection
 * (identified by local/remote tuples of {@link SocketAddress}es) and {@link ProtocolKeyword command type}.
 * Two command latencies are available:
 * <ul>
 * <li>Latency between command send and first response (first response received)</li>
 * <li>Latency between command send and command completion (complete response received)</li>
 * </ul>
 *
 * @author Shashank tyagi
 * @since 0.1
 */
public class CodehaleMetricCollector implements CommandLatencyCollector {


  final private MetricRegistry registry;
  final private CommandLatencyCollectorOptions options;


  public CodehaleMetricCollector(CommandLatencyCollectorOptions options) {
    this(options, new MetricRegistry());
  }

  public CodehaleMetricCollector(CommandLatencyCollectorOptions options,
                                 MetricRegistry registry) {
    this.options = options;
    this.registry = registry;
  }

  /**
   * Record the command latency per {@code connectionPoint} and {@code commandType}.
   *
   * @param local                the local address
   * @param remote               the remote address
   * @param commandType          the command type
   * @param firstResponseLatency latency value in {@link TimeUnit#NANOSECONDS} from send to the first response
   * @param completionLatency    latency value in {@link TimeUnit#NANOSECONDS} from send to the command completion
   */

  public void recordCommandLatency(SocketAddress local,
                                   SocketAddress remote,
                                   ProtocolKeyword commandType,
                                   long firstResponseLatency,
                                   long completionLatency) {
    CommandLatencyId id = createId(local, remote, commandType);
    if (!options.isEnabled()) {
      return;
    }
    registry.timer(
        MetricRegistry.name(
            RedisClient.class,
            id.toString(),
            "firstResponseLatency"
        )
    ).update(firstResponseLatency, TimeUnit.NANOSECONDS);

    registry.timer(
        MetricRegistry.name(
            RedisClient.class,
            id.toString(),
            "completionLatency"
        )
    ).update(firstResponseLatency, TimeUnit.NANOSECONDS);


  }

  private CommandLatencyId createId(SocketAddress local, SocketAddress remote, ProtocolKeyword commandType) {
    return CommandLatencyId.create(options.localDistinction() ? local : LocalAddress.ANY, remote, commandType);
  }

  public void shutdown() {
  }

  @Override
  public Map<CommandLatencyId, CommandMetrics> retrieveMetrics() {
    registry.getTimers((name,m) -> name.contains("Redis"));
    return null;
  }

  public boolean isEnabled() {
    return options.isEnabled();
  }
}
