package com.databricks.apps.logs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;

/**
 * Provides HTML template for Renderer.
 */
public class TemplateProvider {

  public static String fromResource(String fileName) throws IOException {
    try (InputStream input = TemplateProvider.class.getClassLoader().getResourceAsStream(fileName)) {
      if (input == null) {
        throw new IOException("File not found: " + fileName);
      }
      return IOUtils.toString(input, Charset.forName("UTF-8"));
     }
  }
}
