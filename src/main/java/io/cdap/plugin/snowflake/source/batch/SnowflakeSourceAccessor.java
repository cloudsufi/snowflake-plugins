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

package io.cdap.plugin.snowflake.source.batch;

import au.com.bytecode.opencsv.CSVReader;
import io.cdap.plugin.snowflake.common.SnowflakeErrorType;
import io.cdap.plugin.snowflake.common.client.SnowflakeAccessor;
import io.cdap.plugin.snowflake.common.util.DocumentUrlUtil;
import io.cdap.plugin.snowflake.common.util.QueryUtil;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * A class which accesses Snowflake API to do actions used by batch source.
 */
public class SnowflakeSourceAccessor extends SnowflakeAccessor {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeSourceAccessor.class);
  // Directory should be unique, so that parallel pipelines can run correctly, as well as after failure we don't
  // have old stage files in the dir.
  private static final String STAGE_PATH = "@~/cdap_stage/result" + UUID.randomUUID() + "/";
  private static final String COMAND_COPY_INTO =
    "COPY INTO " + STAGE_PATH + "data_ " +
      "FROM (%s) " +
      "FILE_FORMAT=(" +
      "TYPE='CSV' " +
      "COMPRESSION=GZIP " +
      "FIELD_DELIMITER=',' " +
      "ESCAPE=NONE " +
      "ESCAPE_UNENCLOSED_FIELD=NONE " +
      "DATE_FORMAT='YYYY-MM-DD' " +
      "TIME_FORMAT='HH24:MI:SS' " +
      "TIMESTAMP_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SSTZH:TZM' " +
      "FIELD_OPTIONALLY_ENCLOSED_BY='\"' " +
      "NULL_IF='' " +
      "EMPTY_FIELD_AS_NULL=FALSE) " +
      "OVERWRITE=TRUE HEADER=TRUE SINGLE=FALSE";
  private static final String COMMAND_MAX_FILE_SIZE = " MAX_FILE_SIZE=%s";
  private final SnowflakeBatchSourceConfig config;
  private final char escapeChar;

  public SnowflakeSourceAccessor(SnowflakeBatchSourceConfig config, String escapeChar) {
    super(config);
    this.config = config;
    this.escapeChar = escapeChar.charAt(0);
  }

  /**
   * Copies query data into staged files and returns their paths.
   *
   * @return List of file paths in Snowflake stage.
   * @throws IOException thrown if there are any issue with the I/O operations.
   */
  public List<String> prepareStageSplits() {
    LOG.info("Loading data into stage: '{}'", STAGE_PATH);
    String copy = String.format(COMAND_COPY_INTO, QueryUtil.removeSemicolon(config.getImportQuery()));
    if (config.getMaxSplitSize() > 0) {
      copy = copy + String.format(COMMAND_MAX_FILE_SIZE, config.getMaxSplitSize());
    }
    List<String> stageSplits = new ArrayList<>();
    try (Connection connection = dataSource.getConnection();
         PreparedStatement copyStmt = connection.prepareStatement(copy);
         PreparedStatement listStmt = connection.prepareStatement("list " + STAGE_PATH)) {
      copyStmt.execute();
      try (ResultSet resultSet = listStmt.executeQuery()) {
        while (resultSet.next()) {
          String name = resultSet.getString("name");
          stageSplits.add(name);
        }
      }
    } catch (SQLException e) {
      String errorReason = String.format("Failed to load data into stage '%s' with sqlState %s and errorCode %s. " +
          "For more details, see %s.", STAGE_PATH, e.getErrorCode(), e.getSQLState(),
        DocumentUrlUtil.getSupportedDocumentUrl());
      String errorMessage = String.format("Failed to load data into stage '%s' with sqlState %s and errorCode %s. " +
        "Failed to execute query with message: %s.", STAGE_PATH, e.getSQLState(), e.getErrorCode(), e.getMessage());
      throw SnowflakeErrorType.fetchProgramFailureException(e, errorReason, errorMessage);
    }
    return stageSplits;
  }

  /**
   * Remove a file from stage.
   * @param stageSplit  path to file in Snowflake stage.
   * @throws IOException hrown if there are any issue with the I/O operations.
   */
  public void removeStageFile(String stageSplit) {
    runSQL(String.format("remove @~/%s", stageSplit));
  }

  /**
   * Build CSVReader for specified stage split file.
   *
   * @param stageSplit path to file in Snowflake stage.
   * @return CSVReader.
   */
  public CSVReader buildCsvReader(String stageSplit) {
    try (Connection connection = dataSource.getConnection()) {
      InputStream downloadStream = connection.unwrap(SnowflakeConnection.class)
        .downloadStream("@~", stageSplit, true);
      InputStreamReader inputStreamReader = new InputStreamReader(downloadStream);
      return new CSVReader(inputStreamReader, ',', '"', escapeChar);
    } catch (SQLException e) {
      String errorReason = String.format("Failed to execute the query with sqlState: '%s' & errorCode: '%s'. " +
        "For more details, see %s.", e.getSQLState(), e.getErrorCode(), DocumentUrlUtil.getSupportedDocumentUrl());
      String errorMessage = String.format("Failed to execute the query with sqlState: '%s' & errorCode: '%s' " +
          "with message: %s, stage split at %s.", e.getSQLState(), e.getErrorCode(),
        e.getMessage(), stageSplit);
      throw SnowflakeErrorType.fetchProgramFailureException(e, errorReason, errorMessage);
    }
  }
}
