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

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.batch.InputFormatProvider;

import java.util.Map;

/**
 * InputFormatProvider used by cdap to provide configurations to mapreduce job.
 */
public class SnowflakeInputFormatProvider implements InputFormatProvider {

  public static final String PROPERTY_CONFIG_JSON = "cdap.snowflake.source.config";
  public static final String PROPERTY_ESCAPE_CHAR = "cdap.snowflake.source.escape";

  public static final String PROPERTY_DEFAULT_ESCAPE_CHAR = "\\";

  private static final Gson GSON = new Gson();
  private final Map<String, String> conf;

  public SnowflakeInputFormatProvider(SnowflakeBatchSourceConfig config, String escapeChar) {
    this.conf = new ImmutableMap.Builder<String, String>()
      .put(PROPERTY_CONFIG_JSON, GSON.toJson(config))
      .put(PROPERTY_ESCAPE_CHAR, escapeChar)
      .build();
  }

  @Override
  public String getInputFormatClassName() {
    return SnowflakeInputFormat.class.getName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return conf;
  }
}
