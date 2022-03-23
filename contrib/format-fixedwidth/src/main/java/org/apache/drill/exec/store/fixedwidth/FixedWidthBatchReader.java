package org.apache.drill.exec.store.fixedwidth;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileDescrip;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class FixedWidthBatchReader implements ManagedReader {

  private final int maxRecords;  // Do we need this?
  private final FixedWidthFormatConfig config;
  private InputStream fsStream;
  private RowSetLoader loader;
  private CustomErrorContext errorContext;
  private static final Logger logger = LoggerFactory.getLogger(FixedWidthBatchReader.class);
  private BufferedReader reader;

  private int lineNumber;

  /**
   * FixedWidthBatchReader constructor
   * @param negotiator File Schema Negotiator
   * @param config Configuration object
   * @param maxRecords Maximum number of records
   */
  public FixedWidthBatchReader(FileSchemaNegotiator negotiator, FixedWidthFormatConfig config, int maxRecords) {
    System.out.println("FixWidthBatchReader creating");
    this.config = config;
    this.open(negotiator);
    ResultSetLoader setLoader = negotiator.build();
    this.loader = setLoader.writer();
    this.maxRecords = maxRecords;
    System.out.println("FixWidthBatchReader created");
  }

  /**
   * Grab next chunk of the file
   * @return true if successful
   */
  @Override
  public boolean next() {
    System.out.println("next() called");

    while (!this.loader.isFull()) {
      if (!nextLine(this.loader)) {
        return false;
      }
    }
    System.out.println("next() complete");
    return true;
  }

  /**
   * Read in next line
   * @param rowWriter Row set loader
   * @return true if successful
   */
  private boolean nextLine(RowSetLoader rowWriter) {
    String line;

    System.out.println("nextLine() called");

    try {
      line = reader.readLine();
      if (line == null) {
        return false;
      } else if (line.isEmpty()) {
        return true;
      }
    } catch (Exception e) {
      throw UserException.dataReadError(e)
        .message("Error reading file at line number %d", lineNumber)
        .addContext(e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }

    // Start the row
    rowWriter.start();
    try {
      parseLine(line, rowWriter);
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message("Error parsing file at line number %d", lineNumber)
        .addContext(e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }

//    try {
//      parser.parse(line);
//      matchedWriter.setBoolean(true);
//    } catch (Exception e) {
//      errorCount++;
//      if (errorCount >= formatConfig.getMaxErrors()) {
//        throw UserException.dataReadError()
//          .message("Error reading HTTPD file at line number %d", lineNumber)
//          .addContext(e.getMessage())
//          .addContext(errorContext)
//          .build(logger);
//      } else {
//        matchedWriter.setBoolean(false);
//      }
//    }
    // Write raw line
//    rawLineWriter.setString(line);
    // Finish the row
    rowWriter.save();
    lineNumber++;
    return true;
  }

  /**
   * Close the file
   */
  @Override
  public void close() {
    if (this.fsStream != null){
      AutoCloseables.closeSilently(this.fsStream);
      this.fsStream = null;
    }
  }

  /**
   * Open File
   * @param negotiator File Negotiator
   */
  private void open(FileSchemaNegotiator negotiator) {
    System.out.println("open() called");
    this.errorContext = negotiator.parentErrorContext();
    FileDescrip file = negotiator.file();
    this.openFile(file);

    try {
      negotiator.tableSchema(this.buildSchema(), true);

    } catch (Exception e) {
      System.out.println(e.getMessage());

      throw UserException
        .dataReadError(e)
        .message("Failed to open input file: {}", file.split().getPath().toString())
        .addContext(this.errorContext)
        .addContext(e.getMessage())
        .build(FixedWidthBatchReader.logger);
    }
    this.reader = new BufferedReader(new InputStreamReader(this.fsStream, Charsets.UTF_8));
  }

  /**
   * Open file stream.
   * @param file File reference
   */
  private void openFile(FileDescrip file) {
    System.out.println("openFile() called");
    Path filePath = file.split().getPath();
    try {
      this.fsStream = file.fileSystem().openPossiblyCompressedStream(filePath);
    } catch (IOException e) {
      System.out.print("Error: ");
      System.out.println(e.getMessage());
      throw UserException
        .dataReadError(e)
        .message("Unable to open Fixed Width File %s", filePath)
        .addContext(e.getMessage())
        .addContext(this.errorContext)
        .build(FixedWidthBatchReader.logger);
    }
  }

  /**
   * Build the necessary Schema for this file
   * @return Schema as TupleMetadata
   */
  private TupleMetadata buildSchema() {
    SchemaBuilder builder = new SchemaBuilder();
    for (FixedWidthFieldConfig field : config.getFields()) {
      if (field.getType() == TypeProtos.MinorType.VARDECIMAL){
        builder.addNullable(field.getName(), TypeProtos.MinorType.VARDECIMAL,38,4);
        //revisit this
      } else {
        builder.addNullable(field.getName(), field.getType());
      }
    }
    return builder.buildSchema();
  }


  private boolean parseLine(String line, RowSetLoader writer) throws IOException {
    int i = 0;
    TypeProtos.MinorType dataType;
    String dateTimeFormat;
    String value;
    for (FixedWidthFieldConfig field : config.getFields()) {
      value = line.substring(field.getIndex() - 1, field.getIndex() + field.getWidth() - 1);
      dataType = field.getType();
      try {
        switch (dataType) {
          case INT:
            writer.scalar(i).setInt(Integer.parseInt(value));
            break;
          case VARCHAR:
            writer.scalar(i).setString(value);
            break;
          case DATE:
            dateTimeFormat = field.getDateTimeFormat();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateTimeFormat, Locale.ENGLISH);
            LocalDate date = LocalDate.parse(value, formatter);
            writer.scalar(i).setDate(date);
            break;
          case TIME:
            dateTimeFormat = field.getDateTimeFormat();
            DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern(dateTimeFormat, Locale.ENGLISH);
            LocalTime time = LocalTime.parse(value, formatter2);
            writer.scalar(i).setTime(time);
            break;
          case TIMESTAMP:
            dateTimeFormat = field.getDateTimeFormat();
            DateTimeFormatter formatter3 = DateTimeFormatter.ofPattern(dateTimeFormat, Locale.ENGLISH);
            LocalDateTime ldt = LocalDateTime.parse(value, formatter3);
            ZoneId z = ZoneId.of("America/Toronto");
            ZonedDateTime zdt = ldt.atZone(z);
            Instant timeStamp = zdt.toInstant();
            writer.scalar(i).setTimestamp(timeStamp);
            break;
          case FLOAT4:
            writer.scalar(i).setFloat(Float.parseFloat(value));
            break;
          case FLOAT8:
            writer.scalar(i).setDouble(Double.parseDouble(value));
            break;
          case BIGINT:
            writer.scalar(i).setLong(Long.parseLong(value));
            break;
          case VARDECIMAL:
            BigDecimal bigDecimal = new BigDecimal(value);
            writer.scalar(i).setDecimal(bigDecimal);
            break;
          default:
            throw new RuntimeException("Unknown data type specified in fixed width. Found data type " + dataType);
        }
      } catch (RuntimeException e){
        throw new IOException("Failed to parse value: " + value + " as " + dataType);

      }
      i++;
    }
    return true;
  }

}
