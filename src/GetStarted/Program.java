package GetStarted;

import java.io.IOException;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;

public class Program {
    private CosmosClient client;
    private CosmosDatabase database;
    private CosmosContainer container;

    private final String databaseName = "FamilyDB";

    private final String collectionName = "FamilyCollection";

    public static void main(String[] args) {
        final String accountHost = System.getProperty("ACCOUNT_HOST", "").trim();
        final String accountKey = System.getProperty("ACCOUNT_KEY", "").trim();

        if (accountHost.isEmpty()) {
            System.err.println("ACCOUNT_HOST is not set");
            return;
        } else if (accountKey.isEmpty()) {
            System.err.println("ACCOUNT_KEY is not set");
            return;
        }

        try {
            Program p = new Program();
            p.getStartedDemo(accountHost, accountKey);
            System.out.println(String.format("Demo complete, please hold while resources are deleted"));
        } catch (Exception e) {
            System.out.println(String.format("DocumentDB GetStarted failed with %s", e));
        }
    }

    private void getStartedDemo(String accountHost, String accountKey) throws CosmosException, IOException {
        // Create a new CosmosClient via the CosmosClientBuilder
        client = new CosmosClientBuilder()
                .endpoint(accountHost)
                .key(accountKey)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildClient();

        // Create Database
        client.createDatabaseIfNotExists(databaseName);
        database = client.getDatabase(databaseName);

        // Create Container
        database.createContainerIfNotExists(collectionName, "/id", ThroughputProperties.createManualThroughput(400));
        container = database.getContainer(collectionName);

        // Create Item
        Family andersenFamily = getAndersenFamilyDocument();
        container.createItem(andersenFamily);

        Family wakefieldFamily = getWakefieldFamilyDocument();
        container.createItem(wakefieldFamily);

        // Query Item
        String containerSql = String.format("SELECT * from c where c.id = '%s'", collectionName);
        System.out.println(containerSql);

        String itemSql = String.format("SELECT * from %s", collectionName);
        System.out.println(itemSql);

        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        CosmosPagedIterable<CosmosContainerProperties> results = database.queryContainers(containerSql, queryOptions);

        // Iterating through all the results.
        // If there are more than a single page of results, it will auto-magically fetch the next page.
        for (CosmosContainerProperties containerProperties : results) {
            String id = containerProperties.getId();
            CosmosContainer container = database.getContainer(id);
            container.queryItems(itemSql, queryOptions, Family.class).forEach(item -> {
                System.out.println(item.toString());
            });
            System.out.println(id);
        }

        // Delete Item
        database.delete();
    }

    private Family getAndersenFamilyDocument() {
        Family andersenFamily = new Family();
        andersenFamily.setId("Andersen.1");
        andersenFamily.setLastName("Andersen");

        Parent parent1 = new Parent();
        parent1.setFirstName("Thomas");

        Parent parent2 = new Parent();
        parent2.setFirstName("Mary Kay");

        andersenFamily.setParents(new Parent[] { parent1, parent2 });

        Child child1 = new Child();
        child1.setFirstName("Henriette Thaulow");
        child1.setGender("female");
        child1.setGrade(5);

        Pet pet1 = new Pet();
        pet1.setGivenName("Fluffy");

        child1.setPets(new Pet[] { pet1 });

        andersenFamily.setDistrict("WA5");
        Address address = new Address();
        address.setCity("Seattle");
        address.setCounty("King");
        address.setState("WA");

        andersenFamily.setAddress(address);
        andersenFamily.setRegistered(true);

        return andersenFamily;
    }

    private Family getWakefieldFamilyDocument() {
        Family wakefieldFamily = new Family();
        wakefieldFamily.setId("Wakefield.7");
        wakefieldFamily.setLastName("Wakefield");

        Parent parent1 = new Parent();
        parent1.setFamilyName("Wakefield");
        parent1.setFirstName("Robin");

        Parent parent2 = new Parent();
        parent2.setFamilyName("Miller");
        parent2.setFirstName("Ben");

        wakefieldFamily.setParents(new Parent[] { parent1, parent2 });

        Child child1 = new Child();
        child1.setFirstName("Jesse");
        child1.setFamilyName("Merriam");
        child1.setGrade(8);

        Pet pet1 = new Pet();
        pet1.setGivenName("Goofy");

        Pet pet2 = new Pet();
        pet2.setGivenName("Shadow");

        child1.setPets(new Pet[] { pet1, pet2 });

        Child child2 = new Child();
        child2.setFirstName("Lisa");
        child2.setFamilyName("Miller");
        child2.setGrade(1);
        child2.setGender("female");

        wakefieldFamily.setChildren(new Child[] { child1, child2 });

        Address address = new Address();
        address.setCity("NY");
        address.setCounty("Manhattan");
        address.setState("NY");

        wakefieldFamily.setAddress(address);
        wakefieldFamily.setDistrict("NY23");
        wakefieldFamily.setRegistered(true);
        return wakefieldFamily;
    }
}
