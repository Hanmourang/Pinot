package com.linkedin.pinot.trace;

import java.util.concurrent.TimeUnit;


public class MilliSpan implements Span {

  private long startTime;
  private long endTime;
  private long spanId;
  private long traceId;
  private long parentId;
  private String description;

  private MilliSpan(MilliSpanBuilder builder) {
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
    this.spanId = builder.spanId;
    this.traceId = builder.traceId;
    this.parentId = builder.parentId;
    this.description = builder.description;
  }

  public static MilliSpanBuilder newBuilder(long traceId) {
    return new MilliSpanBuilder(traceId);
  }

  @Override
  public long startTime(TimeUnit unit) {
    return unit.convert(startTime, TimeUnit.MILLISECONDS);
  }

  @Override
  public long endTime(TimeUnit unit) {
    return unit.convert(endTime, TimeUnit.MILLISECONDS);
  }

  @Override
  public String description() {
    return this.description;
  }

  @Override
  public long spanId() {
    return this.spanId;
  }

  @Override
  public long traceId() {
    return this.traceId;
  }

  @Override
  public long parentId() {
    return this.parentId;
  }

  public static class MilliSpanBuilder {

    private long startTime;
    private long endTime;
    private long spanId;
    private long traceId;
    private long parentId;
    private String description;


    public MilliSpanBuilder(long traceId) {
      this.traceId = traceId;
    }

    public MilliSpanBuilder startTime(long startTime) {
      this.startTime = startTime;
      return this;
    }

    public MilliSpanBuilder endTime(long endTime) {
      this.endTime = endTime;
      return this;
    }

    public MilliSpanBuilder spanId(long spanId) {
      this.spanId = spanId;
      return this;
    }

    public MilliSpanBuilder parentId(long parentId) {
      this.parentId = parentId;
      return this;
    }

    public MilliSpanBuilder description(String description) {
      this.description = description;
      return this;
    }

    public MilliSpan build() {
      return new MilliSpan(this);
    }
  }
}
