package playground.netflix;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;

import java.util.*;

public class SessionWindowUtil {
  public static void mergeWindows(Collection<SessionWindow> windows, MergingWindowAssigner.MergeCallback<SessionWindow> c) {

    // sort the windows by the start time and then merge overlapping windows

    List<SessionWindow> sortedWindows = new ArrayList<>(windows);

    Collections.sort(sortedWindows, new Comparator<SessionWindow>() {
      @Override
      public int compare(SessionWindow o1, SessionWindow o2) {
        return Long.compare(o1.getStart(), o2.getStart());
      }
    });

    List<Tuple2<SessionWindow, Set<SessionWindow>>> merged = new ArrayList<>();
    Tuple2<SessionWindow, Set<SessionWindow>> currentMerge = null;

    for (SessionWindow candidate : sortedWindows) {
      if (currentMerge == null) {
        currentMerge = new Tuple2<>();
        currentMerge.f0 = candidate;
        currentMerge.f1 = new HashSet<>();
        currentMerge.f1.add(candidate);
      } else if (currentMerge.f0.intersects(candidate)) {
        currentMerge.f0 = currentMerge.f0.cover(candidate);
        currentMerge.f1.add(candidate);
      } else {
        merged.add(currentMerge);
        currentMerge = new Tuple2<>();
        currentMerge.f0 = candidate;
        currentMerge.f1 = new HashSet<>();
        currentMerge.f1.add(candidate);
      }
    }

    if (currentMerge != null) {
      merged.add(currentMerge);
    }

    for (Tuple2<SessionWindow, Set<SessionWindow>> m : merged) {
      if (m.f1.size() > 1) {
        c.merge(m.f1, m.f0);
      }
    }
  }
}