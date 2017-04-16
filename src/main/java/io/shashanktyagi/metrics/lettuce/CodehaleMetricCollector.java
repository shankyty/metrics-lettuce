package io.shashanktyagi.metrics.lettuce;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.metrics.*;
import com.lambdaworks.redis.protocol.ProtocolKeyword;
import io.netty.channel.local.LocalAddress;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * {@link CodehaleMetricCollector} for command latencies which uses Codehale's {@link MetricRegistry}.
 * Command latencies are collected per connection (identified by local/remote tuples of {@link SocketAddress}es)
 * and {@link ProtocolKeyword command type}.
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


  public static final String FIRST_RESPONSE_LATENCY = "firstResponseLatency";
  public static final String COMPLETION_LATENCY = "completionLatency";
  final private MetricRegistry registry;
  final private CommandLatencyCollectorOptions options;
  final private Set<CommandLatencyId> ids = new HashSet<>();


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
            FIRST_RESPONSE_LATENCY
        )
    ).update(firstResponseLatency, TimeUnit.NANOSECONDS);

    registry.timer(
        MetricRegistry.name(
            RedisClient.class,
            id.toString(),
            COMPLETION_LATENCY
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

    Map<CommandLatencyId, CommandMetrics> latencies = getMetrics(options.resetLatenciesAfterEvent());

    return latencies;
  }

  private Map<CommandLatencyId, CommandMetrics> getMetrics(boolean clear) {
    Map<CommandLatencyId, CommandMetrics> latencies = new TreeMap<>();

    for (CommandLatencyId id : ids) {
      Timer firstResponse = registry.timer(
          MetricRegistry.name(
              RedisClient.class,
              id.toString(),
              FIRST_RESPONSE_LATENCY
          )
      );
      Timer completion = registry.timer(
          MetricRegistry.name(
              RedisClient.class,
              id.toString(),
              COMPLETION_LATENCY
          )
      );

      if (firstResponse.getCount() == 0 && completion.getCount() == 0) {
        continue;
      }

      CommandMetrics metrics = new CodeHaleCommandMetrics(firstResponse, completion, options.targetUnit());

      latencies.put(id, metrics);
      if(clear) {
        registry.remove(
            MetricRegistry.name(
                RedisClient.class,
                id.toString(),
                FIRST_RESPONSE_LATENCY
            )
        );
        registry.remove(
            MetricRegistry.name(
                RedisClient.class,
                id.toString(),
                COMPLETION_LATENCY
            )
        );
      }
    }
    return latencies;
  }


  public boolean isEnabled() {
    return options.isEnabled();
  }
}
