package org.apache.spark.network.yarn;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MergedMetricSet implements MetricSet {

  private final Map<String, Metric> allMetrics = new HashMap<>();

  public MergedMetricSet(MetricSet... metricSets) {
    for (MetricSet ms : metricSets) {
      allMetrics.putAll(ms.getMetrics());
    }
  }

  @Override
  public Map<String, Metric> getMetrics() {
    return Collections.unmodifiableMap(allMetrics);
  }
}
