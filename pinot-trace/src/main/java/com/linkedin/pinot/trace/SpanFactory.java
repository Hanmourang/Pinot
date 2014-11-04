package com.linkedin.pinot.trace;

public interface SpanFactory {
  Span newSpan(long traceId, String msg);
}
