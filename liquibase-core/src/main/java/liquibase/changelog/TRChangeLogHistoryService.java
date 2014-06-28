package liquibase.changelog;

import java.util.List;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.exception.DatabaseException;
import liquibase.logging.LogFactory;

public class TRChangeLogHistoryService extends StandardChangeLogHistoryService {

    private List<RanChangeSet> ranChangeSetList;

    private Boolean hasDatabaseChangeLogTable = Boolean.FALSE;

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    public boolean hasDatabaseChangeLogTable() throws DatabaseException {
        if (!hasDatabaseChangeLogTable) {
            hasDatabaseChangeLogTable = super.hasDatabaseChangeLogTable();
        }

        return hasDatabaseChangeLogTable;
    }

    @Override
    public void upgradeChecksums(DatabaseChangeLog databaseChangeLog,
        Contexts contexts,
        LabelExpression labels) throws DatabaseException {
        super.upgradeChecksums(databaseChangeLog, contexts, labels);
        this.ranChangeSetList = null;
    }

    /**
     * Returns the ChangeSets that have been run against the current getDatabase().
     */
    public List<RanChangeSet> getRanChangeSets() throws DatabaseException {
        if (this.ranChangeSetList != null) {
            return this.ranChangeSetList;
        }

        this.ranChangeSetList = super.getRanChangeSets();

        return this.ranChangeSetList;
    }
}
