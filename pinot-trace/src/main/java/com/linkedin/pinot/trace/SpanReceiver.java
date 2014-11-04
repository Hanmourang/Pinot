package com.linkedin.pinot.trace;

import java.io.Closeable;

/**
 * Receive a span
 */
public interface SpanReceiver extends Closeable {

  /**
   * Call whenever span is complete
   * @param span
   */
  void receive(Span span);
}
