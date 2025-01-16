/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.plugin.snowflake.common.util;

/**
 * Utility helper class for Snowflake documentation url
 **/
public final class DocumentUrlUtil {
  // Private constructor to prevent instantiation
  private DocumentUrlUtil() {
    throw new UnsupportedOperationException("Utility class cannot be instantiated");
  }

  /**
   * Supported document URL.
   */
  private static final String SUPPORTED_DOCUMENT_URL =
    "https://docs.snowflake.com/en/user-guide/client-connectivity-troubleshooting/error-messages";

  /**
   * Retrieves the supported document URL.
   *
   * @return the supported document URL
   */
  public static String getSupportedDocumentUrl() {
    return SUPPORTED_DOCUMENT_URL;
  }
}
