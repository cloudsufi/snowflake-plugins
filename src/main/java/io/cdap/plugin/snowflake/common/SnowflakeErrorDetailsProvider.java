/*
 * Copyright © 2024 Cask Data, Inc.
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

import java.net.URISyntaxException;
import java.util.List;


/**
 * Error details provided for the Snowflake
 **/
public class SnowflakeErrorDetailsProvider implements ErrorDetailsProvider {
  @Override
  public ProgramFailureException getExceptionDetails(Exception e, ErrorContext errorContext) {
    List<Throwable> causalChain = Throwables.getCausalChain(e);
    for (Throwable t : causalChain) {
      if (t instanceof ProgramFailureException) {
        // if causal chain already has program failure exception, return null to avoid double wrap.
        return null;
      }
      if (t instanceof IllegalArgumentException) {
        return getProgramFailureException((IllegalArgumentException) t, errorContext);
      }
      if (t instanceof IllegalStateException) {
        return getProgramFailureException((IllegalStateException) t, errorContext);
      }
      if (t instanceof URISyntaxException) {
        return getProgramFailureException((URISyntaxException) t, errorContext);
      }
      if (t instanceof SchemaParseException) {
        return getProgramFailureException((SchemaParseException) t, errorContext);
      }
      if (t instanceof UnexpectedFormatException) {
        return getProgramFailureException((UnexpectedFormatException) t, errorContext);
      }
      if (t instanceof ConnectionTimeoutException) {
        return getProgramFailureException((ConnectionTimeoutException) t, errorContext);
      }
    }
    return null;
  }

  /**
   * Get a ProgramFailureException with the given error
   * information from {@link IllegalArgumentException}.
   *
   * @param e The IllegalArgumentException to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  private ProgramFailureException getProgramFailureException(IllegalArgumentException e, ErrorContext errorContext) {
    String errorMessage = e.getMessage();
    String errorMessageFormat = "Error occurred in the phase: '%s'. Error message: %s";
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
      errorMessage,
      String.format(errorMessageFormat, errorContext.getPhase(), errorMessage), ErrorType.USER, false, e);
  }

  /**
   * Get a ProgramFailureException with the given error
   * information from {@link IllegalStateException}.
   *
   * @param e The IllegalStateException to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  private ProgramFailureException getProgramFailureException(IllegalStateException e, ErrorContext errorContext) {
    String errorMessage = e.getMessage();
    String errorMessageFormat = "Error occurred in the phase: '%s'. Error message: %s";
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
      errorMessage,
      String.format(errorMessageFormat, errorContext.getPhase(), errorMessage), ErrorType.SYSTEM, false, e);
  }

  /**
   * Get a ProgramFailureException with the given error
   * information from {@link URISyntaxException}.
   *
   * @param e The URISyntaxException to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  private ProgramFailureException getProgramFailureException(URISyntaxException e,
                                                             ErrorContext errorContext) {
    String errorMessage = e.getMessage();
    String errorMessageFormat = "Error occurred in the phase: '%s'. Error message: %s";
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
      errorMessage,
      String.format(errorMessageFormat, errorContext.getPhase(), errorMessage), ErrorType.SYSTEM, false, e);
  }

  /**
   * Get a ProgramFailureException with the given error
   * information from {@link SchemaParseException}.
   *
   * @param e The SchemaParseException to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  private ProgramFailureException getProgramFailureException(SchemaParseException e, ErrorContext errorContext) {
    String errorMessage = e.getMessage();
    String errorMessageFormat = "Error occurred in the phase: '%s'. Error message: %s";
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
      errorMessage,
      String.format(errorMessageFormat, errorContext.getPhase(), errorMessage), ErrorType.SYSTEM, false, e);
  }

  /**
   * Get a ProgramFailureException with the given error
   * information from {@link UnexpectedFormatException}.
   *
   * @param e The UnexpectedFormatException to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  private ProgramFailureException getProgramFailureException(UnexpectedFormatException e, ErrorContext errorContext) {
    String errorMessage = e.getMessage();
    String errorMessageFormat = "Error occurred in the phase: '%s'. Error message: %s";
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
      errorMessage,
      String.format(errorMessageFormat, errorContext.getPhase(), errorMessage), ErrorType.SYSTEM, false, e);
  }

  /**
   * Get a ProgramFailureException with the given error
   * information from {@link ConnectionTimeoutException}.
   *
   * @param e The ConnectionTimeoutException to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  private ProgramFailureException getProgramFailureException(ConnectionTimeoutException e, ErrorContext errorContext) {
    String errorMessage = e.getMessage();
    String errorMessageFormat = "Error occurred in the phase: '%s'. Error message: %s";
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
      errorMessage,
      String.format(errorMessageFormat, errorContext.getPhase(), errorMessage), ErrorType.SYSTEM, false, e);
  }
}
