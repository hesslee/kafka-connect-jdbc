/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DatabaseDialect} for Altibase.
 */
public class AltibaseDatabaseDialect extends GenericDatabaseDialect {

  private final Logger log = LoggerFactory.getLogger(AltibaseDatabaseDialect.class);
  
  /**
   * The provider for {@link AltibaseDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(AltibaseDatabaseDialect.class.getSimpleName(), "Altibase");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new AltibaseDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public AltibaseDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  @Override
  protected String currentTimestampDatabaseQuery() {
    return "select CURRENT_DATE from dual";
  }

  @Override
  protected String checkConnectionQuery() {
    return "SELECT 1 FROM DUAL";
  }

  @Override
  public List<TableId> tableIds(Connection conn) throws SQLException {
    DatabaseMetaData metadata = conn.getMetaData();
    String[] tableTypes = tableTypes(metadata, this.tableTypes);
    String tableTypeDisplay = displayableTableTypes(tableTypes, ", ");
    log.debug("Using {} dialect to get {}", this, tableTypeDisplay);

    try (ResultSet rs = metadata.getTables(catalogPattern(), schemaPattern(), "%", tableTypes)) {
      List<TableId> tableIds = new ArrayList<>();
      while (rs.next()) {
        //String catalogName = rs.getString(1);
        String schemaName = rs.getString(2);
        String tableName = rs.getString(3);
        //TableId tableId = new TableId(catalogName, schemaName, tableName);
        TableId tableId = new TableId("", schemaName, tableName);
        if (includeTable(tableId)) {
          tableIds.add(tableId);
        }
      }
      log.debug("Used {} dialect to find {} {}", this, tableIds.size(), tableTypeDisplay);
      return tableIds;
    }
  }
  
  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL(31," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "DATE";
        case Timestamp.LOGICAL_NAME:
          return "DATE";
        default:
          // fall through to normal types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "SMALLINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INTEGER";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "SMALLINT";
      case STRING:
        return "VARCHAR(32672)";
      case BYTES:
        return "BLOB(64000)";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildUpsertQueryStatement(
      final TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    final Transform<ColumnId> transform = (builder, col) -> {
      builder.append(table)
             .append(".")
             .appendColumnName(col.name())
             .append("=DAT.")
             .appendColumnName(col.name());
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("merge into ");
    builder.append(table);
    builder.append(" using (values(");
    builder.appendList()
           .delimitedBy(", ")
           .transformedBy(ExpressionBuilder.placeholderInsteadOfColumnNames("?"))
           .of(keyColumns, nonKeyColumns);
    builder.append(")) as DAT(");
    builder.appendList()
           .delimitedBy(", ")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns, nonKeyColumns);
    builder.append(") on ");
    builder.appendList()
           .delimitedBy(" and ")
           .transformedBy(transform)
           .of(keyColumns);
    if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
      builder.append(" when matched then update set ");
      builder.appendList()
             .delimitedBy(", ")
             .transformedBy(transform)
             .of(nonKeyColumns);
    }

    builder.append(" when not matched then insert(");
    builder.appendList().delimitedBy(",").of(nonKeyColumns, keyColumns);
    builder.append(") values(");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNamesWithPrefix("DAT."))
           .of(nonKeyColumns, keyColumns);
    builder.append(")");
    return builder.toString();
  }
}
