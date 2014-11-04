package com.linkedin.pinot.trace;

import com.google.common.collect.Lists;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;


/**
 * Trace class, not thread safe
 */
public class Trace {

  private final LinkedList<Span> spans;
  private final SpanFactory factory;
  private final List<SpanReceiver> receivers;
  private final long id;

  public Trace(SpanFactory factory) {
    this(null, factory);
  }

  public Trace(Span parent, SpanFactory factory) {
    id = new Random().nextLong();
    spans = Lists.newLinkedList();
    if (parent != null) {
      spans.add(parent);
    }
    this.factory = factory;
    this.receivers = Lists.newArrayList();
  }

  public void addReceiver(SpanReceiver receiver) {
    receivers.add(receiver);
  }

  Span start(String msg) {
    Span span = factory.newSpan(id, msg);
    spans.add(span);
    return span;
  }

  Span end() {
    Span span = EmptySpan.INSTANCE;
    if (spans.size() > 0) {
      span = spans.getLast();
      spans.removeLast();
      for (SpanReceiver sr : receivers) {
        sr.receive(span);
      }
    }
    return span;
  }

  public void example() {
    Trace trace = new Trace(new MilliSpanFactory());
    trace.start("");
    trace.end();
  }
}
