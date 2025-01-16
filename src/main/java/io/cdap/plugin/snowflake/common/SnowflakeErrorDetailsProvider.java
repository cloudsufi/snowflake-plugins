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

package io.cdap.plugin.snowflake.common;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.cdap.etl.api.exception.ErrorDetailsProvider;
import io.cdap.plugin.snowflake.common.exception.ConnectionTimeoutException;
import io.cdap.plugin.snowflake.common.exception.SchemaParseException;

import java.util.List;

/**
 * Error details provided for the Snowflake
 **/
public class SnowflakeErrorDetailsProvider implements ErrorDetailsProvider {

  private static final String ERROR_MESSAGE_FORMAT = "Error occurred in the phase: '%s'. Error message: %s";

  @Override
  public ProgramFailureException getExceptionDetails(Exception e, ErrorContext errorContext) {
    List<Throwable> causalChain = Throwables.getCausalChain(e);
    for (Throwable t : causalChain) {
      if (t instanceof ProgramFailureException) {
        // if causal chain already has program failure exception, return null to avoid double wrap.
        return null;
      }
      if (t instanceof IllegalArgumentException) {
        return getProgramFailureException((IllegalArgumentException) t, errorContext, ErrorType.USER);
      }
      if (t instanceof IllegalStateException) {
        return getProgramFailureException((IllegalStateException) t, errorContext, ErrorType.SYSTEM);
      }
      if (t instanceof SchemaParseException) {
        return getProgramFailureException((SchemaParseException) t, errorContext, ErrorType.USER);
      }
      if (t instanceof UnexpectedFormatException) {
        return getProgramFailureException((UnexpectedFormatException) t, errorContext, ErrorType.SYSTEM);
      }
      if (t instanceof ConnectionTimeoutException) {
        return getProgramFailureException((ConnectionTimeoutException) t, errorContext, ErrorType.SYSTEM);
      }
    }
    return null;
  }

  /**
   * Get a ProgramFailureException with the given error
   * information from {@link Exception}.
   *
   * @param exception The Exception to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  private ProgramFailureException getProgramFailureException(Exception exception, ErrorContext errorContext,
                                                             ErrorType errorType) {
    String errorMessage = exception.getMessage();
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
      errorMessage,
      String.format(ERROR_MESSAGE_FORMAT, errorContext.getPhase(), errorMessage), errorType, false, exception);
  }
}
