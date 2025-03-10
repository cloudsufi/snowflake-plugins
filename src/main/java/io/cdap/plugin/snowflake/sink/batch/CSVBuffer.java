/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.snowflake.sink.batch;

import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/**
 * A buffer which the {@link CSVRecord} is written to before it gets sent to Snowflake.
 */
public class CSVBuffer implements Closeable {
  private static final CSVFormat csvFormat = CSVFormat.DEFAULT
    .withAllowMissingColumnNames(false);

  private CSVPrinter csvPrinter;
  private final ByteArrayOutputStream csvStream;
  private boolean isHeaderPrinted;
  private final boolean printHeader;
  private int recordsCount = 0;

  public CSVBuffer(boolean printHeader) {
    this.printHeader = printHeader;
    this.csvStream = new ByteArrayOutputStream();
    reset();
  }

  public void write(CSVRecord csvRecord) throws IOException {
    if (!isHeaderPrinted) {
      csvPrinter.printRecord(csvRecord.getColumnNames());
      isHeaderPrinted = true;
    }
    csvPrinter.printRecord(csvRecord);
    // unless we flush data from OutputStreamWriter to ByteArrayOutputStream we won't be able to calculate size.
    csvPrinter.flush();
    recordsCount++;
  }

  public void reset() {
    isHeaderPrinted = !printHeader;
    recordsCount = 0;
    csvStream.reset();
    // we need to re-create this or else OutputStreamWriter will not able to write after reset.
    try {
      csvPrinter = new CSVPrinter(new OutputStreamWriter(csvStream, StandardCharsets.UTF_8), csvFormat);
    } catch (IOException e) {
      String errorMessage = String.format("Unable to reset the CSV stream and recreate the CSV printer, " +
        "failed with error message: %s", e.getMessage());
      String errorReason = "Unable to reset the CSV stream and recreate the printer.";
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.UNKNOWN, true, e);
    }
  }

  public int size() {
    return csvStream.size();
  }

  public int getRecordsCount() {
    return recordsCount;
  }

  public byte[] getByteArray() {
    return csvStream.toByteArray();
  }

  public void close() throws IOException {
    csvPrinter.close(true);
  }
}
