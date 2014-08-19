package com.databricks.apps.logs;

import com.google.common.base.Optional;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class Functions {
  public static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

  public static class ValueComparator<K, V>
      implements Comparator<Tuple2<K, V>>, Serializable {
    private Comparator<V> comparator;

    public ValueComparator(Comparator<V> comparator) {
      this.comparator = comparator;
    }

    @Override
    public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
      return comparator.compare(o1._2(), o2._2());
    }
  }

  public static Function2<List<Long>, Optional<Long>, Optional<Long>>
      COMPUTE_RUNNING_SUM = (nums, current) -> {
    long sum = current.or(0L);
    for (long i : nums) {
      sum += i;
    }
    return Optional.of(sum);
  };

}
