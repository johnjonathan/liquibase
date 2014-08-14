package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.database.core.FirebirdDatabase;
import liquibase.statement.core.CreateSchemaDiffControlTableStatement;

public class CreateSchemaDiffControlTableGeneratorFirebird extends CreateSchemaDiffControlTableGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateSchemaDiffControlTableStatement statement, Database database) {
        return database instanceof FirebirdDatabase;
    }
}
