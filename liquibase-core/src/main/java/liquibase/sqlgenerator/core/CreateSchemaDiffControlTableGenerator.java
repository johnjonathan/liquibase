package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.database.core.SybaseDatabase;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.core.CreateSchemaDiffControlTableStatement;
import liquibase.statement.core.CreateTableStatement;

public class CreateSchemaDiffControlTableGenerator extends AbstractSqlGenerator<CreateSchemaDiffControlTableStatement> {

    @Override
    public boolean supports(CreateSchemaDiffControlTableStatement statement, Database database) {
        return (!(database instanceof SybaseDatabase));
    }

    @Override
    public ValidationErrors validate(CreateSchemaDiffControlTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

    @Override
    public Sql[] generateSql(CreateSchemaDiffControlTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        CreateTableStatement createTableStatement = new CreateTableStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getSchemaDiffControlTableName())
                .setTablespace(database.getLiquibaseTablespaceName())
                .addColumn("PATCH_DESCRIPTION", DataTypeFactory.getInstance().fromDescription("VARCHAR(" + getPatchDescriptionColumnSize() + ")", database), null, null, null, new NotNullConstraint())
                .addColumn("PRODUCT_VERSION", DataTypeFactory.getInstance().fromDescription("VARCHAR(" + getProductVersionColumnSize() + ")", database), null, null, null, new NotNullConstraint())
                .addColumn("MODULE", DataTypeFactory.getInstance().fromDescription("VARCHAR(" + getProductVersionColumnSize() + ")", database), new NotNullConstraint())
                .addColumn("PATCH_DATE_EXECUTED", DataTypeFactory.getInstance().fromDescription("date", database))
                .addColumn("LAST_UPDATE", DataTypeFactory.getInstance().fromDescription("date", database));

        return SqlGeneratorFactory.getInstance().generateSql(createTableStatement, database);
    }

    protected String getPatchDescriptionColumnSize() {
        return "50";
    }

    protected String getProductVersionColumnSize() {
        return "15";
    }
    
    protected String getModuleColumnSize() {
        return "50";
    }
}
