package liquibase.schemadiff;

import liquibase.database.Database;
import liquibase.database.core.DerbyDatabase;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.logging.LogFactory;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.statement.core.CreateSchemaDiffControlTableStatement;
import liquibase.statement.core.DropTableStatement;
import liquibase.statement.core.InitializeSchemaDiffControlTableStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.core.Table;

public class StandardSchemaDiffControlService extends AbstractSchemaDiffControlService {

    private Database database;

    private boolean hasSchemaDiffControlTable = false;

    public StandardSchemaDiffControlService() {
    }

    @Override
    public int getPriority() {
        return PRIORITY_DEFAULT;
    }

    @Override
    public boolean supports(Database database) {
        return true;
    }
    
    @Override
    public void reset() {
    
    }


    @Override
    public void setDatabase(Database database) {
        this.database = database;
    }

    @Override
    public void init() throws DatabaseException {

        boolean createdTable = false;
        Executor executor = ExecutorService.getInstance().getExecutor(database);
        if (!hasSchemaDiffControlTable && !hasSchemaDiffControlTable()) {

            executor.comment("Create Schema Diff Control Table");
            executor.execute(new CreateSchemaDiffControlTableStatement());
            database.commit();
            LogFactory
                .getInstance()
                .getLog()
                .debug(
                    "Created schema diff control table with name: " +
                        database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getSchemaDiffControlTableName()));
            this.hasSchemaDiffControlTable = true;
            createdTable = true;
        }

//        if (!isSchemaDiffControlTableInitialized(createdTable)) {
//            executor.comment("Initialize Schema Diff Control Table");
//            executor.execute(new InitializeSchemaDiffControlTableStatement());
//            database.commit();
//        }

        if (executor.updatesDatabase() && database instanceof DerbyDatabase && ((DerbyDatabase) database).supportsBooleanDataType()) { // check if the changelog table is of an old smallint vs. boolean
                                                                                                                                       // format
            String schemaDiffControlTable = database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getSchemaDiffControlTableName());
            Object obj = executor.queryForObject(new RawSqlStatement("select min(last_update) as test from " + schemaDiffControlTable + " fetch first row only"), Object.class);
            if (!(obj instanceof Boolean)) { // wrong type, need to recreate table
                executor.execute(new DropTableStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getSchemaDiffControlTableName(), false));
                executor.execute(new CreateSchemaDiffControlTableStatement());
//                executor.execute(new InitializeSchemaDiffControlTableStatement());
            }
        }

    }

    public boolean isSchemaDiffControlTableInitialized(final boolean tableJustCreated) throws DatabaseException {
        boolean initialized;
        Executor executor = ExecutorService.getInstance().getExecutor(database);
        try {
            initialized =
                executor.queryForInt(new RawSqlStatement("select count(*) from " +
                    database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getSchemaDiffControlTableName()))) > 0;
        } catch (LiquibaseException e) {
            if (executor.updatesDatabase()) {
                throw new UnexpectedLiquibaseException(e);
            } else {
                // probably didn't actually create the table yet.

                initialized = !tableJustCreated;
            }
        }
        return initialized;
    }

    public boolean hasSchemaDiffControlTable() throws DatabaseException {
        boolean hasTable = false;
        try {
            hasTable = SnapshotGeneratorFactory.getInstance().hasSchemaDiffControlTable(database);
        } catch (LiquibaseException e) {
            throw new UnexpectedLiquibaseException(e);
        }
        return hasTable;
    }

    @Override
    public void destroy() throws DatabaseException {
        try {
            if (SnapshotGeneratorFactory.getInstance().has(
                new Table().setName(database.getSchemaDiffControlTableName()).setSchema(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName()), database)) {
                ExecutorService.getInstance().getExecutor(database)
                    .execute(new DropTableStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getSchemaDiffControlTableName(), false));
            }
        } catch (InvalidExampleException e) {
            throw new UnexpectedLiquibaseException(e);
        }

    }

}
