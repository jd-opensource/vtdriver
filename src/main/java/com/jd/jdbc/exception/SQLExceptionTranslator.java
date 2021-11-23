/*
 * Copyright 2002-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.jdbc.exception;

import com.jd.jdbc.sqlparser.utils.StringUtils;
import java.sql.DataTruncation;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.sql.SQLWarning;

public class SQLExceptionTranslator {
    public static SQLException translate(String errorReason, SQLException ex) {
        if (StringUtils.isEmpty(errorReason)) {
            return ex;
        }
        if (ex instanceof SQLNonTransientException) {
            if (ex instanceof SQLNonTransientConnectionException) {
                return ex;
            } else if (ex instanceof SQLDataException) {
                return ex;
            } else if (ex instanceof SQLIntegrityConstraintViolationException) {
                return ex;
            } else if (ex instanceof SQLInvalidAuthorizationSpecException) {
                return ex;
            } else if (ex instanceof SQLSyntaxErrorException) {
                return ex;
            } else if (ex instanceof SQLFeatureNotSupportedException) {
                return ex;
            }
        }

        if (ex instanceof SQLTransientException) {
            if (ex instanceof SQLTransientConnectionException) {
                return new SQLTransientConnectionException(errorReason, ex);
            } else if (ex instanceof SQLTransactionRollbackException) {
                return new SQLTransactionRollbackException(errorReason, ex);
            } else if (ex instanceof SQLTimeoutException) {
                return new SQLTimeoutException(errorReason, ex);
            }
        }

        if (ex instanceof SQLRecoverableException) {
            return new SQLRecoverableException(errorReason, ex);
        }
        if (ex instanceof SQLWarning) {
            if (ex instanceof DataTruncation) {
                return ex;
            }
            return new SQLWarning(errorReason, ex);
        }
        return new SQLException(errorReason, ex);
    }
}