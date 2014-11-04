package com.linkedin.pinot.trace;

import java.util.concurrent.TimeUnit;


public enum EmptySpan implements Span {
  INSTANCE;

  @Override
  public long startTime(TimeUnit unit) {
    return 0;
  }

  @Override
  public long endTime(TimeUnit unit) {
    return 0;
  }

  @Override
  public String description() {
    return null;
  }

  @Override
  public long spanId() {
    return 0;
  }

  @Override
  public long traceId() {
    return 0;
  }

  @Override
  public long parentId() {
    return 0;
  }
}
