package mflix.lessons;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.connection.ClusterSettings;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.autoconfigure.mongo.MongoClientSettingsBuilderCustomizer;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @see com.mongodb.client.MongoClients
 * @see com.mongodb.client.MongoIterable
 * @see com.mongodb.ReadPreference
 * @see org.bson.codecs.Codec
 */
@SpringBootTest
public class MongoClientLesson extends AbstractLesson {

  private MongoClient mongoClient;

  private MongoDatabase database;

  private MongoCollection<Document> collection;

//  private String uri = "mongodb://m220student:m220password@mflix-shard-00-01-3awsg.mongodb.net/mflix?retryWrites=true&w=majority";
  private String uri = "mongodb://m220student:m220password@mflix-shard-00-01-3awsg.mongodb.net:27017,mflix-shard-00-00-3awsg.mongodb.net:27017,mflix-shard-00-02-3awsg.mongodb.net:27017/mflix?replicaSet=MFLIX-SHARD-0&retryWrites=true&w=majority&authSource=admin&&ssl=true&connectTimeoutMS=600000";

  private Document document;

  private Bson bson;

  @Test
  public void MongoClientInstance() {

    /*
    Let's start by instantiating a MongoClient object, since this is the
    pillar of all client (our code) and server (atlas cluster)
    communication.

    To do this we will be using the com.mongodb.client.MongoClients
    builder.

    For this example I'll be using a MongoDB uri that defines to which
    cluster and how we should connect.

    Once I have my connection string, I can go ahead and call
    MongoClients and create() a MongoClient instance, by providing a
    mongodb uri.
     */

     mongoClient = MongoClients.create(uri);

     Assert.assertNotNull(mongoClient);

    MongoClientSettings settings;

    /*
    The MongoClients object will create a MongoClient instance by
    extracting the client connection settings from the connection string.
    However, can also do extended configuration, by setting
    configuration options, that may not be present in the connection
    string uri by setting a MongoClientSettings object.

    This class contains a builder method, a static class method, that
    allows you to compose the different types of client settings upon
    each other.

     */

    mongoClient = getMongoClient();

    Assert.assertNotNull(mongoClient);
  }

  private MongoClient getMongoClient() {
    ConnectionString connectionString = new ConnectionString(uri);
    System.out.println("About to get client settings");
    MongoClientSettings clientSettings = MongoClientSettings.builder()
        .applyConnectionString(connectionString)
        .applicationName("mflix")
        .applyToConnectionPoolSettings(
                (builder) -> {
                  builder.maxWaitTime(60000, TimeUnit.MILLISECONDS);
                  builder.maxConnectionIdleTime(60000, TimeUnit.MILLISECONDS);
                }
        )
            .readPreference(ReadPreference.primaryPreferred())
            .retryWrites(true)
            .applyToSslSettings(builder -> builder.enabled(true))
        .build();
    System.out.println("about to create client");
    return MongoClients.create(clientSettings);
  }

//  private MongoClient getClient() {
//    List<ServerAddress> hosts = new ArrayList<>();
//    Block<ClusterSettings.Builder> clusterBuilder = new B
//    MongoClientSettings clientSettings = MongoClientSettings.builder()
//            .applyToClusterSettings(ClusterSettings.builder().hosts(hosts))
//            .applicationName("mflix")
//            .build()
//    return MongoClients.create(clientSettings);
//    MongoClient mongoClient = new MongoClient(new
//            MongoClientURI("mongodb+srv://wendulem:MYPASSWORD@cluster0
//            ugj9q.mongodb.net/test?retryWrites=true"));

//  }

//    public MongoClient mongoClient() {
//        List<ServerAddress> saList = new ArrayList<>();
//        saList.add(new ServerAddress("mflix-shard-00-00-3awsg.mongodb.net", 27017));
//        saList.add(new ServerAddress("mflix-shard-00-01-3awsg.mongodb.net", 27017));
//        saList.add(new ServerAddress("mflix-shard-00-02-3awsg.mongodb.net", 27017));
//
//        char[] pwd =  "m220password".toCharArray();
//        MongoCredential credential = MongoCredential.createCredential("m220student", "admin", pwd);
//
//        //set sslEnabled to true here
//        MongoClientOptions options = MongoClientOptions.builder()
//                .readPreference(ReadPreference.primaryPreferred())
//                .retryWrites(true)
//                .requiredReplicaSetName("Cluster0-shard-0")
//                .maxConnectionIdleTime(6000)
//                .sslEnabled(true)
//                .build();
//
//        MongoClient mongoClient = new MongoClient(saList, credential, options);
//        return mongoClient;
//    }

  @Test
  public void MongoDatabaseInstance() {

    mongoClient = getMongoClient();

    /*
    Now that we have a MongoClient instance, we can go ahead and connect
    to our cluster and list all available databases in our cluster by
    running the listDatabases command, similar to this mongo shell command:

        db.adminCommand({listDatabases: 1})

     */

    MongoIterable<String> databaseIterable = mongoClient.listDatabaseNames();

    /*
    This command returns a MongoIterable instance, an iterable object
    that we can use to iterate over the results of the a given command.
    We will be using MongoIterable instances quite often.
     */

    List<String> dbnames = new ArrayList<>();
    for (String name : databaseIterable) {
      System.out.println(name);
      dbnames.add(name);
    }

    /*
    Important to note that Iterable instance get exhausted, like a cursor,
    so consider using the iterable instance to fill arrays and lists if
    you need to go over the contents more than once.
     */

    Assert.assertTrue(dbnames.contains("mflix"));

    /*
    Then we have our MongoDatabase object. We will use this object to
    access and create/drop our collections, run commands and define
    database level read preferences, read concerns and write concerns.
     */

    database = mongoClient.getDatabase("mflix");

    ReadPreference readPreference = database.getReadPreference();

    /*
    Here, because we did not specify a Read Preference, we driver will
    use the default configuration which is primary.
     */

    Assert.assertEquals("primary", readPreference.getName());
  }

  @Test
  public void MongoCollectionInstance() {

    /*
    A MongoCollection instance is what is used to read and write to the
    documents, which is usually the entity the application processes to
    manipulate the data it needs.

    To instantiate a collection, we need to provide the
    collection name from a MongoDatabase instance.
     */

//    mongoClient = MongoClients.create(uri);
    mongoClient = getMongoClient();
    database = mongoClient.getDatabase("sample_mflix");
    collection = database.getCollection("movies");

    /*
    In this example we are using the basic form of interacting with
    data defined in a MongoCollection, where we return Document instances
    from any given query.
    However, the MongoCollection allows us to specify Codec so
    that we can return business objects, application defined classes,
    as return of queries. More about this on the POJO support lessons.
     */

    MongoIterable<Document> cursor = collection.find().skip(10).limit(20);

    /*
    All of our Data Manipulation Language (DML) will be expressed via a
    MongoCollection instance;
     */
    List<Document> documents = new ArrayList<>();
    Assert.assertEquals(20, cursor.into(documents).size());
  }

  @Test
  public void DocumentInstance() {
//    mongoClient = MongoClients.create(uri);
    mongoClient = getMongoClient();
    database = mongoClient.getDatabase("test");
//    database.runCommand({ setFeatureCompatibilityVersion: "3.4" } );
    collection = database.getCollection("users");

    /*
    The basic data structures in MongoDB are documents. The document
    model is what we consider to be the best way to represent data.
    Using documents, makes your data definition as close as possible to
    your OOP object models.

    Since we are dealing with an Object-Oriented Programing language (OOP)
    like Java, having a class that expresses the documents structure,
    becomes imperative.
    */

    document = new Document("name", new Document("first", "Norberto").append("last", "Leite"));

    /*
    This document defines a MongoDB document that looks like this in its
    json format:

     {
        "name": {
                "first": "Norberto",
                "last": "Leite"
        }
     }

    */

    collection.insertOne(document);

    /*
    We use documents for everything in MongoDB.
    - define data objects
    - define queries
    - define update operations
    - define configuration settings
    ...

    At the Java layer we have the Document class but also the Bson class.
    The Document class implements the Bson interface, because Documents
    are BSON data structures.
    */

    Assert.assertTrue(document instanceof Bson);

    /*
    We will also use instances of Bson, throughout the course, to define
    fine tune aspects of our queries like query operators and aggregation
    stages. More on that in the next lectures.
    */

  }
}
