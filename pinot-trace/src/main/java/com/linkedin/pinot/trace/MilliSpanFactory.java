package com.linkedin.pinot.trace;

public class MilliSpanFactory implements SpanFactory {
  @Override
  public Span newSpan(long traceId, String msg) {
    return MilliSpan.newBuilder(traceId)
        .startTime(System.currentTimeMillis())
        .description(msg)
        .build();
  }
}
