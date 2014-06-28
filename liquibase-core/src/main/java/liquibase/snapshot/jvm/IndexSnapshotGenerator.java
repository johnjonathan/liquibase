package liquibase.snapshot.jvm;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.core.DB2Database;
import liquibase.database.core.DerbyDatabase;
import liquibase.database.core.InformixDatabase;
import liquibase.database.core.OracleDatabase;
import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.exception.DatabaseException;
import liquibase.snapshot.*;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.*;
import liquibase.util.StringUtils;

import java.sql.DatabaseMetaData;
import java.util.*;

public class IndexSnapshotGenerator extends JdbcSnapshotGenerator {
    public IndexSnapshotGenerator() {
        super(Index.class, new Class[]{Table.class, ForeignKey.class, UniqueConstraint.class});
    }


//    public Boolean has(DatabaseObject example, DatabaseSnapshot snapshot, SnapshotGeneratorChain chain) throws DatabaseException {
//        Database database = snapshot.getDatabase();
//        if (!(example instanceof Index)) {
//            return chain.has(example, snapshot);
//        }
//        String tableName = ((Index) example).getTable().getName();
//        Schema schema = example.getSchema();
//
//        String indexName = example.getName();
//        String columnNames = ((Index) example).getColumnNames();
//
//        try {
//            if (tableName == null) {
//                Index newExample = new Index();
//                newExample.setName(indexName);
//                if (columnNames != null) {
//                    for (String column : columnNames.split("\\s*,\\s*")) {
//                        newExample.getColumns().add(column);
//                    }
//                }
//
//                ResultSet rs = getMetaData(database).getTables(database.getJdbcCatalogName(schema), database.getJdbcSchemaName(schema), null, new String[]{"TABLE"});
//                try {
//                    while (rs.next()) {
//                        String foundTable = rs.getString("TABLE_NAME");
//                        newExample.setTable((Table) new Table().setName(foundTable).setSchema(schema));
//                        if (has(newExample, snapshot, chain)) {
//                            return true;
//                        }
//                    }
//                    return false;
//                } finally {
//                    rs.close();
//                }
//            }
//
//            Index index = new Index();
//            index.setTable((Table) new Table().setName(tableName).setSchema(schema));
//            index.setName(indexName);
//            if (columnNames != null) {
//                for (String column : columnNames.split("\\s*,\\s*")) {
//                    index.getColumns().add(column);
//                }
//            }
//
//            if (columnNames != null) {
//                Map<String, TreeMap<Short, String>> columnsByIndexName = new HashMap<String, TreeMap<Short, String>>();
//                ResultSet rs = getMetaData(database).getIndexInfo(database.getJdbcCatalogName(schema), database.getJdbcSchemaName(schema), database.correctObjectName(tableName, Table.class), false, true);
//                try {
//                    while (rs.next()) {
//                        String foundIndexName = rs.getString("INDEX_NAME");
//                        if (indexName != null && indexName.equalsIgnoreCase(foundIndexName)) { //ok to use equalsIgnoreCase because we will check case later
//                            continue;
//                        }
//                        short ordinalPosition = rs.getShort("ORDINAL_POSITION");
//
//                        if (!columnsByIndexName.containsKey(foundIndexName)) {
//                            columnsByIndexName.put(foundIndexName, new TreeMap<Short, String>());
//                        }
//                        String columnName = rs.getString("COLUMN_NAME");
//                        Map<Short, String> columns = columnsByIndexName.get(foundIndexName);
//                        columns.put(ordinalPosition, columnName);
//                    }
//
//                    for (Map.Entry<String, TreeMap<Short, String>> foundIndexData : columnsByIndexName.entrySet()) {
//                        Index foundIndex = new Index()
//                                .setName(foundIndexData.getKey())
//                                .setTable(((Table) new Table().setName(tableName).setSchema(schema)));
//                        foundIndex.getColumns().addAll(foundIndexData.getValue().values());
//
//                        if (foundIndex.equals(index, database)) {
//                            return true;
//                        }
//                        return false;
//                    }
//                    return false;
//                } finally {
//                    rs.close();
//                }
//            } else if (indexName != null) {
//                ResultSet rs = getMetaData(database).getIndexInfo(database.getJdbcCatalogName(schema), database.getJdbcSchemaName(schema), database.correctObjectName(tableName, Table.class), false, true);
//                try {
//                    while (rs.next()) {
//                        Index foundIndex = new Index()
//                                .setName(rs.getString("INDEX_NAME"))
//                                .setTable(((Table) new Table().setName(tableName).setSchema(schema)));
//                        if (foundIndex.getName() == null) {
//                            continue;
//                        }
//                        if (foundIndex.equals(index, database)) {
//                            return true;
//                        }
//                    }
//                    return false;
//                } finally {
//                    try {
//                        rs.close();
//                    } catch (SQLException ignore) {
//                    }
//                }
//            } else {
//                throw new UnexpectedLiquibaseException("Either indexName or columnNames must be set");
//            }
//        } catch (SQLException e) {
//            throw new DatabaseException(e);
//        }
//
//    }


    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        if (!snapshot.getSnapshotControl().shouldInclude(Index.class)) {
            return;
        }

        if (foundObject instanceof Table) {
            Table table = (Table) foundObject;
            Database database = snapshot.getDatabase();
            Schema schema;
            schema = table.getSchema();


            List<CachedRow> rs = null;
            JdbcDatabaseSnapshot.CachingDatabaseMetaData databaseMetaData = null;
            try {
                databaseMetaData = ((JdbcDatabaseSnapshot) snapshot).getMetaData();

                rs = databaseMetaData.getIndexInfo(((AbstractJdbcDatabase) database).getJdbcCatalogName(schema), ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema), table.getName(), null);
                Map<String, Index> foundIndexes = new HashMap<String, Index>();
                for (CachedRow row : rs) {
                    String indexName = row.getString("INDEX_NAME");
                    if (indexName == null) {
                        continue;
                    }

                     Index index = foundIndexes.get(indexName);
                    if (index == null) {
                        index = new Index();
                        index.setName(indexName);
                        index.setTable(table);
                        foundIndexes.put(indexName, index);
                    }
                    index.getColumns().add(row.getString("COLUMN_NAME"));
                }

                for (Index exampleIndex : foundIndexes.values()) {
                    table.getIndexes().add(exampleIndex);
                }

            } catch (Exception e) {
                throw new DatabaseException(e);
            }
        }
//        if (foundObject instanceof PrimaryKey) {
//            ((PrimaryKey) foundObject).setBackingIndex(new Index().setTable(((PrimaryKey) foundObject).getTable()).setName(foundObject.getName()));
//        }
        if (foundObject instanceof UniqueConstraint && !(snapshot.getDatabase() instanceof DB2Database)&& !(snapshot.getDatabase() instanceof DerbyDatabase)) {
            Index exampleIndex = new Index().setTable(((UniqueConstraint) foundObject).getTable()).setName(foundObject.getName());
            exampleIndex.getColumns().addAll(((UniqueConstraint) foundObject).getColumns());
            ((UniqueConstraint) foundObject).setBackingIndex(exampleIndex);
        }
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        Database database = snapshot.getDatabase();
        Table exampleTable = ((Index) example).getTable();

        String tableName = null;
        Schema schema = null;
        if (exampleTable != null) {
            tableName = exampleTable.getName();
            schema = exampleTable.getSchema();
        }

        if (schema == null) {
            schema = new Schema(database.getDefaultCatalogName(), database.getDefaultSchemaName());
        }


        for (int i=0; i<((Index) example).getColumns().size(); i++) {
            ((Index) example).getColumns().set(i, database.correctObjectName(((Index) example).getColumns().get(i), Column.class));
        }

        String exampleName = example.getName();
        if (exampleName != null) {
            exampleName = database.correctObjectName(exampleName, Index.class);
        }

        Map<String, Index> foundIndexes = new HashMap<String, Index>();
        JdbcDatabaseSnapshot.CachingDatabaseMetaData databaseMetaData = null;
        List<CachedRow> rs = null;
        try {
            databaseMetaData = ((JdbcDatabaseSnapshot) snapshot).getMetaData();

            rs = databaseMetaData.getIndexInfo(((AbstractJdbcDatabase) database).getJdbcCatalogName(schema), ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema), tableName, exampleName);

            for (CachedRow row : rs) {
                String indexName = database.correctObjectName(cleanNameFromDatabase(row.getString("INDEX_NAME"), database), Index.class);
                if (indexName == null) {
                    continue;
                }
                if (database.isCaseSensitive()) {
                    if (exampleName != null && !exampleName.equals(indexName)) {
                        continue;
                    }
                } else {
                    if (exampleName != null && !exampleName.equalsIgnoreCase(indexName)) {
                        continue;
                    }
                }
                /*
                * TODO Informix generates indexnames with a leading blank if no name given.
                * An identifier with a leading blank is not allowed.
                * So here is it replaced.
                */
                if (database instanceof InformixDatabase && indexName.startsWith(" ")) {
                    indexName = "_generated_index_" + indexName.substring(1);
                }
                short type = row.getShort("TYPE");
                //                String tableName = rs.getString("TABLE_NAME");
                Boolean nonUnique = row.getBoolean("NON_UNIQUE");
                if (nonUnique == null) {
                    nonUnique = true;
                }
                String columnName = cleanNameFromDatabase(row.getString("COLUMN_NAME"), database);
                short position = row.getShort("ORDINAL_POSITION");
                /*
                * TODO maybe bug in jdbc driver? Need to investigate.
                * If this "if" is commented out ArrayOutOfBoundsException is thrown
                * because it tries to access an element -1 of a List (position-1)
                */
                if (database instanceof InformixDatabase
                        && type != DatabaseMetaData.tableIndexStatistic
                        && position == 0) {
                    System.out.println(this.getClass().getName() + ": corrected position to " + ++position);
                }
                String filterCondition = StringUtils.trimToNull(row.getString("FILTER_CONDITION"));
                if (filterCondition != null) {
                    filterCondition = filterCondition.replaceAll("\"", "");
                    columnName = filterCondition;
                }

                if (type == DatabaseMetaData.tableIndexStatistic) {
                    continue;
                }
                //                if (type == DatabaseMetaData.tableIndexOther) {
                //                    continue;
                //                }

                if (columnName == null) {
                    //nothing to index, not sure why these come through sometimes
                    continue;
                }
                Index returnIndex = foundIndexes.get(indexName);
                if (returnIndex == null) {
                    returnIndex = new Index();
                    returnIndex.setTable((Table) new Table().setName(row.getString("TABLE_NAME")).setSchema(schema));
                    returnIndex.setName(indexName);
                    returnIndex.setUnique(!nonUnique);
                    foundIndexes.put(indexName, returnIndex);
                }

                for (int i = returnIndex.getColumns().size(); i < position; i++) {
                    returnIndex.getColumns().add(null);
                }
                returnIndex.getColumns().set(position - 1, columnName);
            }
        } catch (Exception e) {
            throw new DatabaseException(e);
        }

        if (exampleName != null) {
            return foundIndexes.get(exampleName);
        } else {
            for (Index index : foundIndexes.values()) {
                if (DatabaseObjectComparatorFactory.getInstance().isSameObject(index.getTable(), exampleTable, database)) {
                    if (database.isCaseSensitive()) {
                        if (index.getColumnNames().equals(((Index) example).getColumnNames())) {
                            return index;
                        }
                    } else {
                        if (index.getColumnNames().equalsIgnoreCase(((Index) example).getColumnNames())) {
                            return index;
                        }
                    }
                }
            }
            return null;
        }
    }

}
