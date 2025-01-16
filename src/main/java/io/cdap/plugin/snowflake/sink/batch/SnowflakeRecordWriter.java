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

import com.google.gson.Gson;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Writes csv records into batches and submits them to Snowflake.
 * Accepts <code>null</code> as a key, and CSVRecord as a value.
 */
public class SnowflakeRecordWriter extends RecordWriter<NullWritable, CSVRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeRecordWriter.class);
  private static final Gson GSON = new Gson();

  private final CSVBuffer csvBuffer;
  private final CSVBuffer csvBufferSizeCheck;
  private final SnowflakeSinkConfig config;
  private final SnowflakeSinkAccessor snowflakeAccessor;
  private final String destinationStagePath;

  public SnowflakeRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    destinationStagePath = conf.get(SnowflakeOutputFormat.DESTINATION_STAGE_PATH_PROPERTY);
    String configJson = conf.get(
      SnowflakeOutputFormatProvider.PROPERTY_CONFIG_JSON);
    config = GSON.fromJson(
      configJson, SnowflakeSinkConfig.class);

    csvBuffer = new CSVBuffer(true);
    csvBufferSizeCheck = new CSVBuffer(false);
    snowflakeAccessor = new SnowflakeSinkAccessor(config);
  }

  @Override
  public void write(NullWritable key, CSVRecord csvRecord) {
    csvBufferSizeCheck.reset();
    try {
      csvBufferSizeCheck.write(csvRecord);
    } catch (IOException e) {
      String errorMessage = String.format("Failed to write CSV record  %s in the size check buffer with message: %s",
        csvRecord, e.getMessage());
      String errorReason = "Unable to write CSV record in the size check buffer";
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }

    if (config.getMaxFileSize() > 0 && csvBuffer.size() + csvBufferSizeCheck.size() > config.getMaxFileSize()) {
      submitCurrentBatch();
    }

    try {
      csvBuffer.write(csvRecord);
    } catch (IOException e) {
      String errorMessage = String.format("Failed to write CSV record  %s in the main buffer with message: %s",
        csvRecord, e.getMessage());
      String errorReason = String.format("Unable to write CSV record '%s' in the main buffer.", csvRecord);
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.SYSTEM, true, e);
    }
  }

  private void submitCurrentBatch() {
    if (csvBuffer.getRecordsCount() != 0) {
      try (InputStream csvInputStream = new ByteArrayInputStream(csvBuffer.getByteArray())) {
        snowflakeAccessor.uploadStream(csvInputStream, destinationStagePath);
      } catch (IOException e) {
        String errorMessage = String.format("Failed to upload file to the destination stage '%s' with message: %s",
          destinationStagePath, e.getMessage());
        String errorReason = String.format("Failed to upload file to the destination stage '%s'.",
          destinationStagePath);
        throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
          errorReason, errorMessage, ErrorType.SYSTEM, true, e);
      }
      csvBuffer.reset();
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    submitCurrentBatch();
  }
}
