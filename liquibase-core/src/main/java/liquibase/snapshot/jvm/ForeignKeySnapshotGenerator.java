package liquibase.snapshot.jvm;

import liquibase.CatalogAndSchema;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.core.MSSQLDatabase;
import liquibase.exception.DatabaseException;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.JdbcDatabaseSnapshot;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.*;

import java.sql.DatabaseMetaData;
import java.util.*;

public class ForeignKeySnapshotGenerator extends JdbcSnapshotGenerator {

    public ForeignKeySnapshotGenerator() {
        super(ForeignKey.class, new Class[] { Table.class});
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        if (!snapshot.getSnapshotControl().shouldInclude(ForeignKey.class)) {
            return;
        }

        if (foundObject instanceof Table) {
            Table table = (Table) foundObject;
            Database database = snapshot.getDatabase();
            Schema schema;
            schema = table.getSchema();


            Set<String> seenFks = new HashSet<String>();
            List<CachedRow> importedKeyMetadataResultSet;
            try {
                importedKeyMetadataResultSet = ((JdbcDatabaseSnapshot) snapshot).getMetaData().getForeignKeys(((AbstractJdbcDatabase) database)
                        .getJdbcCatalogName(schema), ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema),
                        database.correctObjectName(table.getName(), Table.class), null);

                for (CachedRow row : importedKeyMetadataResultSet) {
                    ForeignKey fk = new ForeignKey().setName(row.getString("FK_NAME")).setForeignKeyTable(table);
                    if (seenFks.add(fk.getName())) {
                        table.getOutgoingForeignKeys().add(fk);
                    }
                }
            } catch (Exception e) {
                throw new DatabaseException(e);
            }
        }
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {

        Database database = snapshot.getDatabase();

        List<CachedRow> importedKeyMetadataResultSet;
        try {
            Table fkTable = ((ForeignKey) example).getForeignKeyTable();
            String searchCatalog = ((AbstractJdbcDatabase) database).getJdbcCatalogName(fkTable.getSchema());
            String searchSchema = ((AbstractJdbcDatabase) database).getJdbcSchemaName(fkTable.getSchema());
            String searchTableName = database.correctObjectName(fkTable.getName(), Table.class);

            importedKeyMetadataResultSet = ((JdbcDatabaseSnapshot) snapshot).getMetaData().getForeignKeys(searchCatalog, searchSchema, searchTableName, example.getName());
            ForeignKey foreignKey = null;
            for (CachedRow row : importedKeyMetadataResultSet) {
                String fk_name = cleanNameFromDatabase(row.getString("FK_NAME"), database);
                if (snapshot.getDatabase().isCaseSensitive()) {
                    if (!fk_name.equals(example.getName())) {
                        continue;
                    } else if (!fk_name.equalsIgnoreCase(example.getName())) {
                        continue;
                    }
                }

                if (foreignKey == null) {
                    foreignKey = new ForeignKey();
                }

                foreignKey.setName(fk_name);

                String fkTableCatalog = cleanNameFromDatabase(row.getString("FKTABLE_CAT"), database);
                String fkTableSchema = cleanNameFromDatabase(row.getString("FKTABLE_SCHEM"), database);
                String fkTableName = cleanNameFromDatabase(row.getString("FKTABLE_NAME"), database);
                Table foreignKeyTable = new Table().setName(fkTableName);
                foreignKeyTable.setSchema(new Schema(new Catalog(fkTableCatalog), fkTableSchema));

                foreignKey.setForeignKeyTable(foreignKeyTable);
                foreignKey.addForeignKeyColumn(cleanNameFromDatabase(row.getString("FKCOLUMN_NAME"), database));

                CatalogAndSchema pkTableSchema = ((AbstractJdbcDatabase) database).getSchemaFromJdbcInfo(row.getString("PKTABLE_CAT"), row.getString("PKTABLE_SCHEM"));
                Table tempPkTable = (Table) new Table().setName(row.getString("PKTABLE_NAME")).setSchema(new Schema(pkTableSchema.getCatalogName(), pkTableSchema.getSchemaName()));
                foreignKey.setPrimaryKeyTable(tempPkTable);
                foreignKey.addPrimaryKeyColumn(cleanNameFromDatabase(row.getString("PKCOLUMN_NAME"), database));
                //todo foreignKey.setKeySeq(importedKeyMetadataResultSet.getInt("KEY_SEQ"));

                ForeignKeyConstraintType updateRule = convertToForeignKeyConstraintType(row.getInt("UPDATE_RULE"), database);
                foreignKey.setUpdateRule(updateRule);
                ForeignKeyConstraintType deleteRule = convertToForeignKeyConstraintType(row.getInt("DELETE_RULE"), database);
                foreignKey.setDeleteRule(deleteRule);
                short deferrability = row.getShort("DEFERRABILITY");
                // Hsqldb doesn't handle setting this property correctly, it sets it to 0.
                // it should be set to DatabaseMetaData.importedKeyNotDeferrable(7)
                if (deferrability == 0 || deferrability == DatabaseMetaData.importedKeyNotDeferrable) {
                    foreignKey.setDeferrable(false);
                    foreignKey.setInitiallyDeferred(false);
                } else if (deferrability == DatabaseMetaData.importedKeyInitiallyDeferred) {
                    foreignKey.setDeferrable(true);
                    foreignKey.setInitiallyDeferred(true);
                } else if (deferrability == DatabaseMetaData.importedKeyInitiallyImmediate) {
                    foreignKey.setDeferrable(true);
                    foreignKey.setInitiallyDeferred(false);
                } else {
                    throw new RuntimeException("Unknown deferrability result: " + deferrability);
                }

                if (database.createsIndexesForForeignKeys()) {
                    Index exampleIndex = new Index().setTable(foreignKey.getForeignKeyTable());
                    exampleIndex.getColumns().addAll(Arrays.asList(foreignKey.getForeignKeyColumns().split("\\s*,\\s*")));
                    foreignKey.setBackingIndex(exampleIndex);
                }
            }
            return foreignKey;
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }


    protected ForeignKeyConstraintType convertToForeignKeyConstraintType(Integer jdbcType, Database database) throws DatabaseException {
        if (jdbcType == null) {
            return ForeignKeyConstraintType.importedKeyRestrict;
        }
        if (database instanceof MSSQLDatabase) {
            /*
                 * The sp_fkeys stored procedure spec says that returned integer values of 0, 1 and 2
     * translate to cascade, noAction and SetNull, which are not the values in the JDBC
     * standard. This override is a sticking plaster to stop invalid SQL from being generated.
             */
            if (jdbcType == 0) {
                return ForeignKeyConstraintType.importedKeyCascade;
            } else if (jdbcType == 1) {
                return ForeignKeyConstraintType.importedKeyNoAction;
            } else if (jdbcType == 2) {
                return ForeignKeyConstraintType.importedKeySetNull;
            } else {
                throw new DatabaseException("Unknown constraint type: " + jdbcType);
            }
        } else {
            if (jdbcType == DatabaseMetaData.importedKeyCascade) {
                return ForeignKeyConstraintType.importedKeyCascade;
            } else if (jdbcType == DatabaseMetaData.importedKeyNoAction) {
                return ForeignKeyConstraintType.importedKeyNoAction;
            } else if (jdbcType == DatabaseMetaData.importedKeyRestrict) {
                return ForeignKeyConstraintType.importedKeyRestrict;
            } else if (jdbcType == DatabaseMetaData.importedKeySetDefault) {
                return ForeignKeyConstraintType.importedKeySetDefault;
            } else if (jdbcType == DatabaseMetaData.importedKeySetNull) {
                return ForeignKeyConstraintType.importedKeySetNull;
            } else {
                throw new DatabaseException("Unknown constraint type: " + jdbcType);
            }
        }
    }
   
}
