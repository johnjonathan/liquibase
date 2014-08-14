package liquibase.schemadiff;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import liquibase.database.Database;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.servicelocator.ServiceLocator;

/**
 * @author John Sanda
 */
public class SchemaDiffControlServiceFactory {

	private static SchemaDiffControlServiceFactory instance;

	private List<SchemaDiffControlService> registry = new ArrayList<SchemaDiffControlService>();

	private Map<Database, SchemaDiffControlService> services = new ConcurrentHashMap<Database, SchemaDiffControlService>();

	public static synchronized SchemaDiffControlServiceFactory getInstance() {
		if (instance == null) {
			instance = new SchemaDiffControlServiceFactory();
		}
		return instance;
	}

    /**
     * Set the instance used by this singleton. Used primarily for testing.
     */
    public static void setInstance(SchemaDiffControlServiceFactory schemaDiffControlServiceFactory) {
        SchemaDiffControlServiceFactory.instance = schemaDiffControlServiceFactory;
    }


    public static void reset() {
        instance = null;
    }

    private SchemaDiffControlServiceFactory() {
		Class<? extends SchemaDiffControlService>[] classes;
		try {
			classes = ServiceLocator.getInstance().findClasses(SchemaDiffControlService.class);

			for (Class<? extends SchemaDiffControlService> clazz : classes) {
				register(clazz.getConstructor().newInstance());
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void register(SchemaDiffControlService schemaDiffControlService) {
		registry.add(0, schemaDiffControlService);
	}

	
	public SchemaDiffControlService getSchemaDiffControlService(Database database) {
        if (services.containsKey(database)) {
            return services.get(database);
        }
        SortedSet<SchemaDiffControlService> foundServices = new TreeSet<SchemaDiffControlService>(new Comparator<SchemaDiffControlService>() {
            @Override
            public int compare(SchemaDiffControlService o1, SchemaDiffControlService o2) {
                return -1 * new Integer(o1.getPriority()).compareTo(o2.getPriority());
            }
        });

        for (SchemaDiffControlService service : registry) {
            if (service.supports(database)) {
                foundServices.add(service);
            }
        }

        if (foundServices.size() == 0) {
            throw new UnexpectedLiquibaseException("Cannot find SchemaDiffControlService for " + database.getShortName());
        }

        try {
            SchemaDiffControlService exampleService = foundServices.iterator().next();
            Class<? extends SchemaDiffControlService> aClass = exampleService.getClass();
            SchemaDiffControlService service;
            try {
                aClass.getConstructor();
                service = aClass.newInstance();
                service.setDatabase(database);
            } catch (NoSuchMethodException e) {
                // must have been manually added to the registry and so already configured.
                service = exampleService;
            }

            services.put(database, service);
            return service;
        } catch (Exception e) {
            throw new UnexpectedLiquibaseException(e);
        }
}

	public void resetAll() {
		for (SchemaDiffControlService schemaDiffControlService : registry) {
			schemaDiffControlService.reset();
		}
		instance = null;
	}

}
