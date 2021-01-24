package com.flipkart.dsp;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import java.io.IOException;

public class TestUtils {

  public static String fixture(String filename) {
    try {
      return Resources.toString(Resources.getResource(filename), Charsets.UTF_8).trim();
    } catch (IOException var3) {
      throw new IllegalArgumentException(var3);
    }
  }
}
