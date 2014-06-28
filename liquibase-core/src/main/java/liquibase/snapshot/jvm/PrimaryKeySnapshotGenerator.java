package liquibase.snapshot.jvm;

import liquibase.CatalogAndSchema;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.core.MSSQLDatabase;
import liquibase.database.core.SQLiteDatabase;
import liquibase.exception.DatabaseException;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.JdbcDatabaseSnapshot;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class PrimaryKeySnapshotGenerator extends JdbcSnapshotGenerator {

    public PrimaryKeySnapshotGenerator() {
        super(PrimaryKey.class, new Class[]{Table.class});
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        Database database = snapshot.getDatabase();
        Schema schema = example.getSchema();
        String searchTableName = null;
        if (((PrimaryKey) example).getTable() != null) {
            searchTableName = ((PrimaryKey) example).getTable().getName();
            searchTableName = database.correctObjectName(searchTableName, Table.class);
        }

        List<CachedRow> rs = null;
        try {
            JdbcDatabaseSnapshot.CachingDatabaseMetaData metaData = ((JdbcDatabaseSnapshot) snapshot).getMetaData();
            rs = metaData.getPrimaryKeys(((AbstractJdbcDatabase) database).getJdbcCatalogName(schema), ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema), searchTableName);
            PrimaryKey returnKey = null;
            for (CachedRow row : rs) {
                if (example.getName() != null && !example.getName().equals(row.getString("PK_NAME"))) {
                    continue;
                }
                String columnName = cleanNameFromDatabase(row.getString("COLUMN_NAME"), database);
                short position = row.getShort("KEY_SEQ");

                if (returnKey == null) {
                    returnKey = new PrimaryKey();
                    CatalogAndSchema tableSchema = ((AbstractJdbcDatabase) database).getSchemaFromJdbcInfo(row.getString("TABLE_CAT"), row.getString("TABLE_SCHEM"));
                    returnKey.setTable((Table) new Table().setName(row.getString("TABLE_NAME")).setSchema(new Schema(tableSchema.getCatalogName(), tableSchema.getSchemaName())));
                    returnKey.setName(row.getString("PK_NAME"));
                }

                if (database instanceof SQLiteDatabase) { //SQLite is zero based position?
                    position = (short) (position + 1);
                }

                returnKey.addColumnName(position - 1, columnName);
            }

            if (returnKey != null) {
                Index exampleIndex = new Index().setTable(returnKey.getTable());
                exampleIndex.getColumns().addAll(Arrays.asList(returnKey.getColumnNames().split("\\s*,\\s*")));
                if (database instanceof MSSQLDatabase) { //index name matches PK name for better accuracy
                    exampleIndex.setName(returnKey.getName());
                }
                returnKey.setBackingIndex(exampleIndex);
            }

            return returnKey;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        if (!snapshot.getSnapshotControl().shouldInclude(PrimaryKey.class)) {
            return;
        }

        if (foundObject instanceof Table) {
            Table table = (Table) foundObject;
            Database database = snapshot.getDatabase();
            Schema schema = table.getSchema();

            List<CachedRow> rs = null;
            try {
                JdbcDatabaseSnapshot.CachingDatabaseMetaData metaData = ((JdbcDatabaseSnapshot) snapshot).getMetaData();
                rs = metaData.getPrimaryKeys(((AbstractJdbcDatabase) database).getJdbcCatalogName(schema), ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema), table.getName());
                if (rs.size() > 0) {
                    table.setPrimaryKey(new PrimaryKey().setName(rs.get(0).getString("PK_NAME")).setTable(table));
                }
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }

        }
    }
 
}
