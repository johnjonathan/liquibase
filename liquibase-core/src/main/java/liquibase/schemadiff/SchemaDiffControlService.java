package liquibase.schemadiff;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.servicelocator.PrioritizedService;

/**
 * @author John Sanda
 */
public interface SchemaDiffControlService extends PrioritizedService {

    boolean supports(Database database);

    void setDatabase(Database database);

    /**
     * Clears information the lock handler knows about the tables.  Should only be called by Liquibase internal calls
     */
    void reset();

    void init() throws DatabaseException;

    void destroy() throws DatabaseException;
}
