package com.flipkart.dsp.exception;

public class TableRefreshException extends RuntimeException{

  public TableRefreshException(String message) {
    super(message);
  }

  public TableRefreshException(String message, Throwable cause) {
    super(message, cause);
  }
}
