package com.databricks.apps.logs

import java.io.IOException

import scala.io.Source

/** Provides HTML template for Renderer. */
object TemplateProvider {

  /**
    * Read content of a file as a single string. The file is looked as a resource in classpath.
    *
    * @param fileName File name.
    * @return File content as a string.
    * @throws IOException Failure during file read.
    */
  def fromResource(fileName: String): String = {
    import resource._
    val res = managed(getClass.getClassLoader.getResourceAsStream(fileName))
      .map {
        inputStream =>
          if (inputStream == null)
            throw new IOException("File not found: " + fileName)
          val template = Source.fromInputStream(inputStream).mkString
          template
      }.either.either
    res match {
      case Left(throwables) => throw throwables.head
      case Right(string) => string
    }
  }
}
