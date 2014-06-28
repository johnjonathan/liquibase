package liquibase.snapshot.jvm;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.database.jvm.JdbcConnection;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.*;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.logging.LogFactory;
import liquibase.snapshot.*;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.*;
import liquibase.util.ISODateFormat;
import liquibase.util.StringUtils;

import java.math.BigDecimal;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Scanner;

public class ColumnSnapshotGenerator extends JdbcSnapshotGenerator {

    public ColumnSnapshotGenerator() {
        super(Column.class, new Class[]{Table.class, View.class});
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        Database database = snapshot.getDatabase();
        Relation relation = ((Column) example).getRelation();
        Schema schema = relation.getSchema();

        List<CachedRow> columnMetadataRs = null;
        try {

            JdbcDatabaseSnapshot.CachingDatabaseMetaData databaseMetaData = ((JdbcDatabaseSnapshot) snapshot).getMetaData();

            columnMetadataRs = databaseMetaData.getColumns(((AbstractJdbcDatabase) database).getJdbcCatalogName(schema), ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema), relation.getName(), example.getName());

            if (columnMetadataRs.size() > 0) {
                CachedRow data = columnMetadataRs.get(0);
                return readColumn(data, relation, database);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        if (!snapshot.getSnapshotControl().shouldInclude(Column.class)) {
            return;
        }
        if (foundObject instanceof Relation) {
            Database database = snapshot.getDatabase();
            Relation relation = (Relation) foundObject;
            List<CachedRow> allColumnsMetadataRs = null;
            try {

                JdbcDatabaseSnapshot.CachingDatabaseMetaData databaseMetaData = ((JdbcDatabaseSnapshot) snapshot).getMetaData();

                Schema schema;

                schema = relation.getSchema();
                allColumnsMetadataRs = databaseMetaData.getColumns(((AbstractJdbcDatabase) database).getJdbcCatalogName(schema), ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema), relation.getName(), null);

                for (CachedRow row : allColumnsMetadataRs) {
                    Column exampleColumn = new Column().setRelation(relation).setName(row.getString("COLUMN_NAME"));
                    relation.getColumns().add(exampleColumn);
                }
            } catch (Exception e) {
                throw new DatabaseException(e);
            }
        }

    }

    protected Column readColumn(CachedRow columnMetadataResultSet, Relation table, Database database) throws SQLException, DatabaseException {
        String rawTableName = (String) columnMetadataResultSet.get("TABLE_NAME");
        String rawColumnName = (String) columnMetadataResultSet.get("COLUMN_NAME");
        String rawSchemaName = StringUtils.trimToNull((String) columnMetadataResultSet.get("TABLE_SCHEM"));
        String rawCatalogName = StringUtils.trimToNull((String) columnMetadataResultSet.get("TABLE_CAT"));
        String remarks = StringUtils.trimToNull((String) columnMetadataResultSet.get("REMARKS"));
        if (remarks != null) {
            remarks = remarks.replace("''", "'"); //come back escaped sometimes
        }


        Column column = new Column();
        column.setName(rawColumnName);
        column.setRelation(table);
        column.setRemarks(remarks);

        if (database instanceof OracleDatabase) {
            String nullable = columnMetadataResultSet.getString("NULLABLE");
            if (nullable.equals("Y")) {
                column.setNullable(true);
            } else {
                column.setNullable(false);
            }
        } else {
            int nullable = columnMetadataResultSet.getInt("NULLABLE");
            if (nullable == DatabaseMetaData.columnNoNulls) {
                column.setNullable(false);
            } else if (nullable == DatabaseMetaData.columnNullable) {
                column.setNullable(true);
            } else if (nullable == DatabaseMetaData.columnNullableUnknown) {
                LogFactory.getInstance().getLog().info("Unknown nullable state for column " + column.toString() + ". Assuming nullable");
                column.setNullable(true);
            }
        }

        if (database.supportsAutoIncrement()) {
            if (table instanceof Table) {
                if (columnMetadataResultSet.containsColumn("IS_AUTOINCREMENT")) {
                    String isAutoincrement = (String) columnMetadataResultSet.get("IS_AUTOINCREMENT");
                    isAutoincrement = StringUtils.trimToNull(isAutoincrement);
                    if (isAutoincrement == null) {
                        column.setAutoIncrementInformation(null);
                    } else if (isAutoincrement.equals("YES")) {
                        column.setAutoIncrementInformation(new Column.AutoIncrementInformation());
                    } else if (isAutoincrement.equals("NO")) {
                        column.setAutoIncrementInformation(null);
                    } else if (isAutoincrement.equals("")) {
                        LogFactory.getInstance().getLog().info("Unknown auto increment state for column " + column.toString() + ". Assuming not auto increment");
                        column.setAutoIncrementInformation(null);
                    } else {
                        throw new UnexpectedLiquibaseException("Unknown is_autoincrement value: '" + isAutoincrement+"'");
                    }
                } else {
                    //probably older version of java, need to select from the column to find out if it is auto-increment
                    String selectStatement = "select " + database.escapeColumnName(rawCatalogName, rawSchemaName, rawTableName, rawColumnName) + " from " + database.escapeTableName(rawCatalogName, rawSchemaName, rawTableName) + " where 0=1";
                    LogFactory.getInstance().getLog().debug("Checking "+rawTableName+"."+rawCatalogName+" for auto-increment with SQL: '"+selectStatement+"'");
                    Connection underlyingConnection = ((JdbcConnection) database.getConnection()).getUnderlyingConnection();
                    Statement statement = null;
                    ResultSet columnSelectRS = null;

                    try {
                        statement = underlyingConnection.createStatement();
                        columnSelectRS = statement.executeQuery(selectStatement);
                        if (columnSelectRS.getMetaData().isAutoIncrement(1)) {
                            column.setAutoIncrementInformation(new Column.AutoIncrementInformation());
                        } else {
                            column.setAutoIncrementInformation(null);
                        }
                    } finally {
                        try {
                            if (statement != null) {
                                statement.close();
                            }
                        } catch (SQLException ignore) {
                        }
                        if (columnSelectRS != null) {
                            columnSelectRS.close();
                        }
                    }
                }
            }
        }

        DataType type = readDataType(columnMetadataResultSet, column, database);
        column.setType(type);

        column.setDefaultValue(readDefaultValue(columnMetadataResultSet, column, database));

        return column;
    }

    protected DataType readDataType(CachedRow columnMetadataResultSet, Column column, Database database) throws SQLException {

        if (database instanceof OracleDatabase) {
            String dataType = columnMetadataResultSet.getString("DATA_TYPE");
            dataType = dataType.replace("VARCHAR2", "VARCHAR");
            dataType = dataType.replace("NVARCHAR2", "NVARCHAR");

            DataType type = new DataType(dataType);
//            type.setDataTypeId(dataType);
            if (dataType.equalsIgnoreCase("NUMBER")) {
                type.setColumnSize(columnMetadataResultSet.getInt("DATA_PRECISION"));
//                if (type.getColumnSize() == null) {
//                    type.setColumnSize(38);
//                }
                type.setDecimalDigits(columnMetadataResultSet.getInt("DATA_SCALE"));
//                if (type.getDecimalDigits() == null) {
//                    type.setDecimalDigits(0);
//                }
//            type.setRadix(10);
            } else {
                type.setColumnSize(columnMetadataResultSet.getInt("DATA_LENGTH"));

                if (dataType.equalsIgnoreCase("NCLOB")) {
                    //no attributes
                } else if (dataType.equalsIgnoreCase("NVARCHAR") || dataType.equalsIgnoreCase("NCHAR")) {
                    //data length is in bytes but specified in chars
                    type.setColumnSize(type.getColumnSize() / 2);
                    type.setColumnSizeUnit(DataType.ColumnSizeUnit.CHAR);
                } else {
                    String charUsed = columnMetadataResultSet.getString("CHAR_USED");
                    DataType.ColumnSizeUnit unit = null;
                    if ("C".equals(charUsed)) {
                        unit = DataType.ColumnSizeUnit.CHAR;
                        type.setColumnSize(type.getColumnSize() / 2);
                    }
                    type.setColumnSizeUnit(unit);
                }
            }


            return type;
        }

        String columnTypeName = (String) columnMetadataResultSet.get("TYPE_NAME");

        if (database instanceof FirebirdDatabase) {
            if (columnTypeName.equals("BLOB SUB_TYPE 0")) {
                columnTypeName = "BLOB";
            }
            if (columnTypeName.equals("BLOB SUB_TYPE 1")) {
                columnTypeName = "CLOB";
            }
        }

        if (database instanceof MySQLDatabase && (columnTypeName.equalsIgnoreCase("ENUM") || columnTypeName.equalsIgnoreCase("SET"))) {
            try {
                String boilerLength;
                if (columnTypeName.equalsIgnoreCase("ENUM"))
                    boilerLength = "7";
                else // SET
                    boilerLength = "6";
                List<String> enumValues = ExecutorService.getInstance().getExecutor(database).queryForList(new RawSqlStatement("SELECT DISTINCT SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(COLUMN_TYPE, " + boilerLength + ", LENGTH(COLUMN_TYPE) - " + boilerLength + " - 1 ), \"','\", 1 + units.i + tens.i * 10) , \"','\", -1)\n" +
                        "FROM INFORMATION_SCHEMA.COLUMNS\n" +
                        "CROSS JOIN (SELECT 0 AS i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) units\n" +
                        "CROSS JOIN (SELECT 0 AS i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) tens\n" +
                        "WHERE TABLE_NAME = '"+column.getRelation().getName()+"' \n" +
                        "AND COLUMN_NAME = '"+column.getName()+"'"), String.class);
                String enumClause = "";
                for (String enumValue : enumValues) {
                    enumClause += "'"+enumValue+"', ";
                }
                enumClause = enumClause.replaceFirst(", $", "");
                return new DataType(columnTypeName + "("+enumClause+")");
            } catch (DatabaseException e) {
                LogFactory.getInstance().getLog().warning("Error fetching enum values", e);
            }
        }
        DataType.ColumnSizeUnit columnSizeUnit = DataType.ColumnSizeUnit.BYTE;

        int dataType = columnMetadataResultSet.getInt("DATA_TYPE");
        Integer columnSize = columnMetadataResultSet.getInt("COLUMN_SIZE");
        // don't set size for types like int4, int8 etc
        if (database.dataTypeIsNotModifiable(columnTypeName)) {
            columnSize = null;
        }

        Integer decimalDigits = columnMetadataResultSet.getInt("DECIMAL_DIGITS");
        if (decimalDigits != null && decimalDigits.equals(0)) {
            decimalDigits = null;
        }

        Integer radix = columnMetadataResultSet.getInt("NUM_PREC_RADIX");

        Integer characterOctetLength = columnMetadataResultSet.getInt("CHAR_OCTET_LENGTH");

        if (database instanceof DB2Database) {
            String typeName = columnMetadataResultSet.getString("TYPE_NAME");
            if (typeName.equalsIgnoreCase("DBCLOB") || typeName.equalsIgnoreCase("GRAPHIC") || typeName.equalsIgnoreCase("VARGRAPHIC")) {
                if (columnSize != null) {
                    columnSize = columnSize / 2; //Stored as double length chars
                }
            }
        }


        DataType type = new DataType(columnTypeName);
        type.setDataTypeId(dataType);
        type.setColumnSize(columnSize);
        type.setDecimalDigits(decimalDigits);
        type.setRadix(radix);
        type.setCharacterOctetLength(characterOctetLength);
        type.setColumnSizeUnit(columnSizeUnit);

        return type;
    }

    protected Object readDefaultValue(CachedRow columnMetadataResultSet, Column columnInfo, Database database) throws SQLException, DatabaseException {
        if (database instanceof MSSQLDatabase) {
            Object defaultValue = columnMetadataResultSet.get("COLUMN_DEF");

            if (defaultValue != null && defaultValue instanceof String) {
                if (defaultValue.equals("(NULL)")) {
                    columnMetadataResultSet.set("COLUMN_DEF", null);
                }
            }
        }

        if (database instanceof OracleDatabase) {
            if (columnMetadataResultSet.get("COLUMN_DEF") == null) {
                columnMetadataResultSet.set("COLUMN_DEF", columnMetadataResultSet.get("DATA_DEFAULT"));
            }

        }

        Object val = columnMetadataResultSet.get("COLUMN_DEF");
        if (!(val instanceof String)) {
            return val;
        }

        int type = Integer.MIN_VALUE;
        if (columnInfo.getType().getDataTypeId() != null) {
            type = columnInfo.getType().getDataTypeId();
        }
        String typeName = columnInfo.getType().getTypeName();

        LiquibaseDataType liquibaseDataType = DataTypeFactory.getInstance().from(columnInfo.getType(), database);

        String stringVal = (String) val;
        if (stringVal.isEmpty()) {
            if (liquibaseDataType instanceof CharType) {
                return "";
            } else {
                return null;
            }
        }


        if (database instanceof OracleDatabase && !stringVal.startsWith("'") && !stringVal.endsWith("'")) {
            //oracle returns functions without quotes
            Object maybeDate = null;

            if (liquibaseDataType instanceof DateType || type == Types.DATE) {
                if (stringVal.endsWith("'HH24:MI:SS')")) {
                    maybeDate = DataTypeFactory.getInstance().fromDescription("time", database).sqlToObject(stringVal, database);
                } else {
                    maybeDate = DataTypeFactory.getInstance().fromDescription("date", database).sqlToObject(stringVal, database);
                }
            } else if (liquibaseDataType instanceof DateTimeType || type == Types.TIMESTAMP) {
                maybeDate = DataTypeFactory.getInstance().fromDescription("datetime", database).sqlToObject(stringVal, database);
            } else {
                return new DatabaseFunction(stringVal);
            }
            if (maybeDate != null) {
                if (maybeDate instanceof java.util.Date) {
                    return maybeDate;
                } else {
                    return new DatabaseFunction(stringVal);
                }
            }
        }

        if (stringVal.startsWith("'") && stringVal.endsWith("'")) {
            stringVal = stringVal.substring(1, stringVal.length() - 1);
        } else if (stringVal.startsWith("((") && stringVal.endsWith("))")) {
            stringVal = stringVal.substring(2, stringVal.length() - 2);
        } else if (stringVal.startsWith("('") && stringVal.endsWith("')")) {
            stringVal = stringVal.substring(2, stringVal.length() - 2);
        } else if (stringVal.startsWith("(") && stringVal.endsWith(")")) {
            return new DatabaseFunction(stringVal.substring(1, stringVal.length() - 1));
        }

        Scanner scanner = new Scanner(stringVal.trim());
        if (type == Types.ARRAY) {
            return new DatabaseFunction(stringVal);
        } else if ((liquibaseDataType instanceof BigIntType || type == Types.BIGINT)) {
            if (scanner.hasNextBigInteger()) {
                return scanner.nextBigInteger();
            } else {
                return new DatabaseFunction(stringVal);
            }
        } else if (type == Types.BINARY) {
            return new DatabaseFunction(stringVal.trim());
        } else if (type == Types.BIT) {
            if (stringVal.startsWith("b'")) { //mysql returns boolean values as b'0' and b'1'
                stringVal = stringVal.replaceFirst("b'", "").replaceFirst("'$", "");
            }
            stringVal = stringVal.trim();
            if (scanner.hasNextBoolean()) {
                return scanner.nextBoolean();
            } else {
                return new Integer(stringVal);
            }
        } else if (liquibaseDataType instanceof BlobType|| type == Types.BLOB) {
            return new DatabaseFunction(stringVal);
        } else if ((liquibaseDataType instanceof BooleanType || type == Types.BOOLEAN )) {
            if (scanner.hasNextBoolean()) {
                return scanner.nextBoolean();
            } else {
                return new DatabaseFunction(stringVal);
            }
        } else if (liquibaseDataType instanceof CharType || type == Types.CHAR) {
            return stringVal;
        } else if (liquibaseDataType instanceof ClobType || type == Types.CLOB) {
            return stringVal;
        } else if (type == Types.DATALINK) {
            return new DatabaseFunction(stringVal);
        } else if (liquibaseDataType instanceof DateType || type == Types.DATE) {
            if (typeName.equalsIgnoreCase("year")) {
                return stringVal.trim();
            }
            return DataTypeFactory.getInstance().fromDescription("date", database).sqlToObject(stringVal, database);
        } else if ((liquibaseDataType instanceof DecimalType || type == Types.DECIMAL)) {
            if (scanner.hasNextBigDecimal()) {
                return scanner.nextBigDecimal();
            } else {
                return new DatabaseFunction(stringVal);
            }
        } else if (type == Types.DISTINCT) {
            return new DatabaseFunction(stringVal);
        } else if ((liquibaseDataType instanceof DoubleType || type == Types.DOUBLE)) {
            if (scanner.hasNextDouble()) {
                return scanner.nextDouble();
            } else {
                return new DatabaseFunction(stringVal);
            }
        } else if ((liquibaseDataType instanceof FloatType || type == Types.FLOAT)) {
            if (scanner.hasNextFloat()) {
                return scanner.nextFloat();
            } else {
                return new DatabaseFunction(stringVal);
            }
        } else if ((liquibaseDataType instanceof IntType || type == Types.INTEGER)) {
            if (scanner.hasNextInt()) {
                return scanner.nextInt();
            } else {
                return new DatabaseFunction(stringVal);
            }
        } else if (type == Types.JAVA_OBJECT) {
            return new DatabaseFunction(stringVal);
        } else if (type == Types.LONGNVARCHAR) {
            return stringVal;
        } else if (type == Types.LONGVARBINARY) {
            return new DatabaseFunction(stringVal);
        } else if (type == Types.LONGVARCHAR) {
            return stringVal;
        } else if (liquibaseDataType instanceof NCharType || type == Types.NCHAR) {
            return stringVal;
        } else if (type == Types.NCLOB) {
            return stringVal;
        } else if (type == Types.NULL) {
            return null;
        } else if ((liquibaseDataType instanceof NumberType || type == Types.NUMERIC)) {
            if (scanner.hasNextBigDecimal()) {
                return scanner.nextBigDecimal();
            } else {
                return new DatabaseFunction(stringVal);
            }
        } else if (liquibaseDataType instanceof NVarcharType || type == Types.NVARCHAR) {
            return stringVal;
        } else if (type == Types.OTHER) {
            if (database instanceof DB2Database && typeName.equalsIgnoreCase("DECFLOAT")) {
                return new BigDecimal(stringVal);
            }
            return new DatabaseFunction(stringVal);
        } else if (type == Types.REAL) {
            return new BigDecimal(stringVal.trim());
        } else if (type == Types.REF) {
            return new DatabaseFunction(stringVal);
        } else if (type == Types.ROWID) {
            return new DatabaseFunction(stringVal);
        } else if ((liquibaseDataType instanceof SmallIntType || type == Types.SMALLINT)) {
            if (scanner.hasNextInt()) {
                return scanner.nextInt();
            } else {
                return new DatabaseFunction(stringVal);
            }
        } else if (type == Types.SQLXML) {
            return new DatabaseFunction(stringVal);
        } else if (type == Types.STRUCT) {
            return new DatabaseFunction(stringVal);
        } else if (liquibaseDataType instanceof TimeType || type == Types.TIME) {
            return DataTypeFactory.getInstance().fromDescription("time", database).sqlToObject(stringVal, database);
        } else if (liquibaseDataType instanceof DateTimeType || liquibaseDataType instanceof TimestampType || type == Types.TIMESTAMP) {
            return DataTypeFactory.getInstance().fromDescription("datetime", database).sqlToObject(stringVal, database);
        } else if ((liquibaseDataType instanceof TinyIntType || type == Types.TINYINT)) {
            if (scanner.hasNextInt()) {
                return scanner.nextInt();
            } else {
                return new DatabaseFunction(stringVal);
            }
        } else if (type == Types.VARBINARY) {
            return new DatabaseFunction(stringVal);
        } else if (liquibaseDataType instanceof VarcharType || type == Types.VARCHAR) {
            return stringVal;
        } else if (database instanceof MySQLDatabase && typeName.toLowerCase().startsWith("enum")) {
            return stringVal;
        } else {
            LogFactory.getInstance().getLog().info("Unknown default value: value '" + stringVal + "' type " + typeName + " (" + type + "), assuming it is a function");
            return new DatabaseFunction(stringVal);
        }

    }
  
}
