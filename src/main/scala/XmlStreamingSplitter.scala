import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}
import java.io._
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.collection.mutable.StringBuilder

/**
 * Streaming XML Splitter for large XML files (30GB+)
 * 
 * This splitter uses StAX (Streaming API for XML) to process enormous XML files
 * without loading them into memory. It detects configurable row tags and extracts
 * each into a separate XML chunk file for parallel Spark processing.
 * 
 * Usage:
 *   scala XmlStreamingSplitter <inputFile> <outputDir> <rowTag> [maxRecordsPerFile]
 * 
 * Example:
 *   scala XmlStreamingSplitter data.xml chunks/ Record 1000
 */
object XmlStreamingSplitter {

  case class Config(
    inputFile: String,
    outputDir: String,
    rowTag: String,
    maxRecordsPerFile: Int = 1000,
    targetChunkSizeMB: Int = 50
  )

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: XmlStreamingSplitter <inputFile> <outputDir> <rowTag> [maxRecordsPerFile]")
      System.err.println("Example: XmlStreamingSplitter data.xml chunks/ Record 1000")
      System.exit(1)
    }

    val config = Config(
      inputFile = args(0),
      outputDir = args(1),
      rowTag = args(2),
      maxRecordsPerFile = if (args.length > 3) args(3).toInt else 1000
    )

    println(s"[XML Splitter] Starting XML chunking process")
    println(s"[XML Splitter] Input: ${config.inputFile}")
    println(s"[XML Splitter] Output: ${config.outputDir}")
    println(s"[XML Splitter] Row tag: <${config.rowTag}>")
    println(s"[XML Splitter] Max records per file: ${config.maxRecordsPerFile}")
    println()

    val startTime = System.currentTimeMillis()
    
    try {
      splitXml(config)
      
      val duration = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"\n[XML Splitter] ✓ Completed successfully in ${duration}s")
    } catch {
      case e: Exception =>
        System.err.println(s"\n[XML Splitter] ✗ FAILED: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    }
  }

  def splitXml(config: Config): Unit = {
    // Create output directory
    val outputPath = Paths.get(config.outputDir)
    Files.createDirectories(outputPath)
    println(s"[XML Splitter] Output directory ready: ${config.outputDir}")

    // Initialize StAX reader
    val factory = XMLInputFactory.newInstance()
    factory.setProperty(XMLInputFactory.IS_COALESCING, true) // Merge adjacent text nodes
    factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false) // Simplify namespace handling
    
    val inputStream = new BufferedInputStream(new FileInputStream(config.inputFile), 8 * 1024 * 1024) // 8MB buffer
    val reader = factory.createXMLStreamReader(inputStream)

    var fileIndex = 0
    var recordCount = 0
    var totalRecords = 0
    var currentWriter: Option[BufferedWriter] = None
    var insideRowTag = false
    var depth = 0
    var recordDepth = 0
    val recordBuffer = new StringBuilder(1024 * 100) // 100KB initial buffer per record
    
    var rootElementName: Option[String] = None
    var rootAttributes = Map.empty[String, String]

    try {
      while (reader.hasNext) {
        val eventType = reader.next()

        eventType match {
          case XMLStreamConstants.START_ELEMENT =>
            val localName = reader.getLocalName
            
            // Capture root element for wrapping chunks
            if (depth == 0) {
              rootElementName = Some(localName)
              rootAttributes = extractAttributes(reader)
            }

            // Detect start of row tag
            if (localName == config.rowTag && !insideRowTag) {
              insideRowTag = true
              recordDepth = depth
              recordBuffer.clear()
              
              // Create new file if needed
              if (recordCount == 0 || recordCount >= config.maxRecordsPerFile) {
                // Close previous writer
                currentWriter.foreach { w =>
                  writeFooter(w, rootElementName)
                  w.close()
                }
                
                // Open new writer
                fileIndex += 1
                val chunkFile = outputPath.resolve(f"part-$fileIndex%05d.xml").toFile
                val writer = new BufferedWriter(new FileWriter(chunkFile), 1024 * 1024) // 1MB buffer
                currentWriter = Some(writer)
                
                writeHeader(writer, rootElementName, rootAttributes)
                recordCount = 0
                
                if (fileIndex % 10 == 0) {
                  println(s"[XML Splitter] Created chunk file #$fileIndex (total records: $totalRecords)")
                }
              }
            }

            // Write element to buffer if inside row tag
            if (insideRowTag) {
              recordBuffer.append("<").append(localName)
              
              // Write attributes
              for (i <- 0 until reader.getAttributeCount) {
                val attrName = reader.getAttributeLocalName(i)
                val attrValue = escapeXml(reader.getAttributeValue(i))
                recordBuffer.append(s""" $attrName="$attrValue"""")
              }
              
              recordBuffer.append(">")
            }

            depth += 1

          case XMLStreamConstants.END_ELEMENT =>
            depth -= 1
            val localName = reader.getLocalName

            if (insideRowTag) {
              recordBuffer.append("</").append(localName).append(">")
              
              // Check if we've closed the row tag
              if (localName == config.rowTag && depth == recordDepth) {
                insideRowTag = false
                
                // Write complete record to file
                currentWriter.foreach { w =>
                  w.write("  ")
                  w.write(recordBuffer.toString)
                  w.write("\n")
                }
                
                recordCount += 1
                totalRecords += 1
                
                // Progress indicator
                if (totalRecords % 10000 == 0) {
                  print(s"\r[XML Splitter] Processed: $totalRecords records")
                  System.out.flush()
                }
                
                recordBuffer.clear()
              }
            }

          case XMLStreamConstants.CHARACTERS =>
            if (insideRowTag) {
              val text = reader.getText
              if (text != null && text.trim.nonEmpty) {
                recordBuffer.append(escapeXml(text))
              }
            }
          
          case 12 => // CDATA_SECTION constant value
            if (insideRowTag) {
              val text = reader.getText
              if (text != null && text.trim.nonEmpty) {
                recordBuffer.append(escapeXml(text))
              }
            }

          case XMLStreamConstants.COMMENT =>
            // Skip comments to reduce output size

          case XMLStreamConstants.PROCESSING_INSTRUCTION =>
            // Skip processing instructions

          case _ =>
            // Ignore other event types
        }
      }

      // Close final writer
      currentWriter.foreach { w =>
        writeFooter(w, rootElementName)
        w.close()
      }

      println(s"\r[XML Splitter] Processed: $totalRecords records (complete)")
      println(s"[XML Splitter] Created $fileIndex chunk files")
      println(s"[XML Splitter] Average records per file: ${totalRecords.toDouble / fileIndex}")

    } finally {
      reader.close()
      inputStream.close()
      currentWriter.foreach(_.close())
    }
  }

  private def extractAttributes(reader: XMLStreamReader): Map[String, String] = {
    (0 until reader.getAttributeCount).map { i =>
      reader.getAttributeLocalName(i) -> reader.getAttributeValue(i)
    }.toMap
  }

  private def writeHeader(writer: BufferedWriter, rootElement: Option[String], attrs: Map[String, String]): Unit = {
    writer.write("""<?xml version="1.0" encoding="UTF-8"?>""")
    writer.write("\n")
    
    rootElement.foreach { name =>
      writer.write(s"<$name")
      attrs.foreach { case (k, v) =>
        writer.write(s""" $k="${escapeXml(v)}"""")
      }
      writer.write(">\n")
    }
  }

  private def writeFooter(writer: BufferedWriter, rootElement: Option[String]): Unit = {
    rootElement.foreach { name =>
      writer.write(s"</$name>\n")
    }
    writer.flush()
  }

  private def escapeXml(text: String): String = {
    text
      .replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
      .replace("'", "&apos;")
  }
}
