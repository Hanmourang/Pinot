package com.linkedin.pinot.trace;

import java.util.concurrent.TimeUnit;


public interface Span {

  long startTime(TimeUnit unit);

  long endTime(TimeUnit unit);

  String description();

  long spanId();

  long traceId();

  long parentId();
}
