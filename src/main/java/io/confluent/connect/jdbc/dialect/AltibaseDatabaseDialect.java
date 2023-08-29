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

// 다른 DB들의 dialect들을 참고하여 적당히 만들었음.
// 프로토타입 형태로써, 검증이 매우 부족한 상태임.
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
    //buildUpsertQueryStatement 함수의 구현문제로, 테이블이름 및 컬럼이름을 쌍따옴표 처리하지 않습니다.
    //super(config, new IdentifierRules(".", "\"", "\""));
    IdentifierRules rules = new IdentifierRules(".", "\"", "\"");
    rules.setQuoteIdentifiers(NEVER);
    super(config, rules);
    //이것만으로는 안됨... altibase source or sink connector 생성시에 아래 설정을 추가해야 함.
    //"quote.sql.identifiers": "NEVER"
  }

  @Override
  protected String currentTimestampDatabaseQuery() {
    return "select CURRENT_DATE from dual";
  }

  @Override
  protected String checkConnectionQuery() {
    return "SELECT 1 FROM DUAL";
  }

  // SELECT * FROM "mydb"."user_name"."table_name" 문제
  // Altibase JDBC DatabaseMetaData.getTables(...) 함수에서 TABLE_CAT 로  "mydb"를 리턴해서 발생하는 문제임.
  // PostgreSQL에서는 위 함수에서 TABLE_CAT 로  NULL을 리턴해서 문제가 없음.
  // 알티베이스 JDBC는 그대로 두고, 아래 함수에서 NULL처리해서 해결함....
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

  // kafka topic을 이용해서, table auto creation 할때 사용되는 부분인데, 검증하지는 않았음.
  // kafka topic에서 string 이 있을때, 자릿수가 없으므로, altibase 입장에서는 최대크기인 VARCHAR(32000) 로 생성함.
  // 다른 타입들도 동일한 논리로, precision, scale 등을 알티베이스의 최대값으로 해주고 있음.
  // 다른 DB들의 dialect 에도 동일하게 해당 DB의 최대값으로 해주고 있음.
  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          // Altibase max precision is 38
          return "DECIMAL(38," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
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
        return "VARCHAR(32000)";
      case BYTES:
        return "BLOB";
      default:
        return super.getSqlType(field);
    }
  }

  // 기존 merge into 구문 문제를 해결하고자, anonymous block 기능을 이용하여, insert 후에 duplication 시에 update 하는 형태를 구현함.
  // 이 방식에서, recordtype을 이용하는데, recordtype에서 대소문자 컬럼처리에 문제가 있어서, 테이블이름 및 컬럼이름을 쌍따옴표 처리하지 않습니다.
  // 즉, 대소문자 테이블이름 및 컬럼이름 사용이 안되고, 모두 대문자 처리하기 위하여,
  // source 및 sink connector의 설정에 아래 사항을 추가해야 합니다.
  // "quote.sql.identifiers": "NEVER"
  
  // 기존 merge into 구문 문제 :
  // merge into "test-altibase-USERS" using (select ? "ID", ? "NAME" FROM dual) incoming on("test-altibase-USERS"."ID"=incoming."ID") 
  // when matched then update set "test-altibase-USERS"."NAME"=incoming."NAME" 
  // when not matched then insert("test-altibase-USERS"."NAME","test-altibase-USERS"."ID") values(incoming."NAME",incoming."ID")
  // ERROR-0x3123B : Invalid use of host variables
  // 알티베이스는 select target 절에, 호스트변수를 쓸려면, cast를 해야한다... ***** 이거 뭐 할때마다 걸린다... ***제품개선 1순위***
  // 위의 경우 (select case(? as varchar(20)) "ID", case(? as int) "NAME" FROM dual) 요런 형태로 바뀌어야 한다.
  // 위와같이 변경을 해주어도, cast()에서 LOB을 지원하지 않으므로, insert.mode=upsert 방식에서는 알티베이스는 LOB은 지원할수 없음.
  @Override
  public String buildUpsertQueryStatement(
      final TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    final Transform<ColumnId> transformAssignment = (builder, col) -> {
      builder.append("r1.")
             .appendColumnName(col.name())
             .append(" := ? ; ");
    };
    final Transform<ColumnId> transformUpdate = (builder, col) -> {
      builder.appendColumnName(col.name())
             .append(" = r1.")
             .appendColumnName(col.name());
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("declare ");
    builder.append(" r1 ").append(table).append("%rowtype; ");
    builder.append(" begin ");
    builder.appendList()
           .delimitedBy(" ")
           .transformedBy(transformAssignment)
           .of(keyColumns, nonKeyColumns);
    builder.append(" insert into ").append(table).append(" values r1; ");

    if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
      builder.append(" exception ");
      builder.append(" when dup_val_on_index then ");
      builder.append(" update ").append(table).append(" set ");
      builder.appendList()
             .delimitedBy(", ")
             .transformedBy(transformUpdate)
             .of(nonKeyColumns);
      builder.append(" where ");
      builder.appendList()
             .delimitedBy(" and ")
             .transformedBy(transformUpdate)
             .of(keyColumns);
      builder.append(" ; ");
    }

    builder.append(" end; ");
    return builder.toString();
  }

}
