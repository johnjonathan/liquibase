package liquibase.integration.ant;

import java.io.Writer;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;

import org.apache.tools.ant.BuildException;

public class ChangeLogSyncTask extends BaseLiquibaseTask {

    @Override
    public void executeWithLiquibaseClassloader() throws BuildException {
        if (!shouldRun()) {
            return;
        }

        Liquibase liquibase = null;
        try {
            liquibase = createLiquibase();

            Writer writer = createOutputWriter();
            if (writer == null) {
                liquibase.changeLogSync(new Contexts(getContexts()), new LabelExpression(getLabels()));
            } else {
                liquibase.changeLogSync(new Contexts(getContexts()), new LabelExpression(getLabels()), writer);
                writer.flush();
                writer.close();
            }

        } catch (Exception e) {
            throw new BuildException(e);
        } finally {
            closeDatabase(liquibase);
        }
    }
}