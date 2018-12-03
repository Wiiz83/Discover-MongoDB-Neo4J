package mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import utils.CLIUtils;

public class Main {

    private static MongoDatabase currentDatabaseName;
    private static MongoCollection<Document> currentCollectionName;
    private static MongoClient mongoClient;
    private static CLIUtils cliUtils = new CLIUtils();

    public static void main(String[] args) {
        String uri = "mongodb://127.0.0.1:27017";
        MongoClientURI connectionString = new MongoClientURI(uri);
        mongoClient = new MongoClient(connectionString);

        currentDatabaseName = null;
        currentCollectionName = null;

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        displayMenu();
    }

    private static void displayMenu() {
        System.out.println("Choisir une base de données : 1 \n" +
                "Choisir une collection : 2 \n" +
                "Lister les documents disponibles : 3 \n" +
                "Rechercher un document à partir d'un nom : 4 \n" +
                "Ajouter un nouveau document : 5 \n" +
                "Compter les documents : 6\n" +
                "Supprimer une base de données : 7");
        Scanner scanner = new Scanner(System.in);
        ArrayList<Integer> choix = new ArrayList<Integer>() {{
            add(new Integer(1));
            add(new Integer(2));
            add(new Integer(3));
            add(new Integer(4));
            add(new Integer(5));
            add(new Integer(6));
            add(new Integer(7));
        }};
        Integer c = cliUtils.saisirEntier(scanner, "Choix: \n", choix);
        switch (c) {
            case 1:
                displayDatabases();
                break;
            case 2:
                displayCollections();
                break;
            case 3:
                displayDocuments();
                break;
            case 4:
                searchDocumentByName();
                break;
            case 5:
                insertDocument();
                break;
            case 6:
                countDocuments();
                break;
            case 7 :
                supprDB();
                break;
        }
    }

    private static void displayDatabases() {
        MongoCursor<String> dbsCursor = mongoClient.listDatabaseNames().iterator();
        Map<Integer, String> allDbs = new HashMap<Integer, String>();
        ArrayList<Integer> choix = new ArrayList<Integer>();
        System.out.println("\nBase de données disponibles :");
        int number = 1;
        while (dbsCursor.hasNext()) {
            choix.add(new Integer(number));
            String name = dbsCursor.next().toString();
            System.out.println(number + " : " + name);
            allDbs.put(number, name);
            number++;
        }

        Scanner scanner = new Scanner(System.in);
        Integer key = cliUtils.saisirEntier(scanner, "Choix: \n", choix);
        System.out.println();
        if (!allDbs.get(key).isEmpty()) {
            currentDatabaseName = mongoClient.getDatabase(allDbs.get(key));
            displayMenu();
        } else {
            System.out.println("Cette base n'existe pas.\n");
            displayDatabases();
        }
    }

    private static void displayCollections() {
        checkDatabase();

        MongoCursor<String> collecCursor = currentDatabaseName.listCollectionNames().iterator();
        Map<Integer, String> allCollec = new HashMap<Integer, String>();
        ArrayList<Integer> choix = new ArrayList<Integer>();
        System.out.println("\nCollections disponibles pour la base " + currentDatabaseName.getName() + " :");
        int number = 1;
        while (collecCursor.hasNext()) {
            choix.add(new Integer(number));
            String name = collecCursor.next().toString();
            System.out.println(number + " : " + name);
            allCollec.put(number, name);
            number++;
        }

        Scanner scanner = new Scanner(System.in);
        Integer key = cliUtils.saisirEntier(scanner, "Choix: \n", choix);
        System.out.println();
        if (!allCollec.get(key).isEmpty()) {
            currentCollectionName = currentDatabaseName.getCollection(allCollec.get(key));
            displayMenu();
        } else {
            System.out.println("Cette collection n'existe pas.\n");
            displayCollections();
        }
    }

    private static void displayDocuments() {
        checkDatabase();
        checkCollection();

        FindIterable<Document> documents = currentCollectionName.find();
        System.out.println("\nListe des documents disponibles :");
        for (Document doc : documents) {
            System.out.println(doc.toJson(JsonWriterSettings.builder().indent(true).build()));
        }
        System.out.println();
        displayMenu();
    }

    private static void searchDocumentByName() {
        checkDatabase();
        checkCollection();

        Scanner scanner = new Scanner(System.in);
        String key = cliUtils.saisirChaine(scanner, "\nEntrez le nom de la personne a rechercher : \n");

        Document document = currentCollectionName.find(Filters.eq("name", key)).first(); //'"'+key+'"'
        if (document != null) {
            System.out.println("Document sélectionné :");
            System.out.println(document.toJson(JsonWriterSettings.builder().indent(true).build()));
            displayMenu();
        } else {
            System.out.println("Aucun document ne correspond à ce critère de recherche. Veuillez reessayer.");
            searchDocumentByName();
        }
    }

    private static void checkCollection() {
        if (currentCollectionName == null) {
            System.out.println("Vous devez choisir une collection d'abord.\n");
            displayMenu();
        }
    }

    private static void insertDocument() {
        checkDatabase();
        checkCollection();
        //System.out.println("currentCollectionName.getNamespace() : " + currentCollectionName.getNamespace());
        if (currentCollectionName.getNamespace().toString().equals("dbcontacts.contactsReduits")) {
            Document doc = new Document();
            Scanner scanner = new Scanner(System.in);
            String nom = cliUtils.saisirChaine(scanner, "\nSaisissez le nom de la personne à insérer : \n");
            String sexe;
            do {
                sexe = cliUtils.saisirChaine(scanner, "\nSaisissez le sexe de la personne à insérer (H/F) : \n");
            }
            while (!sexe.equals("H") && !sexe.equals("F"));
            Long age = cliUtils.saisirEntier(scanner, "\nSaisissez l'age de la personne à insérer : \n", new Long(0), new Long(100));
            String coord = cliUtils.saisirChaine(scanner, "\nSaisissez les coordonnées GPS (format : LONG.#.LAT) : \n");
            String niv = cliUtils.saisirChaine(scanner, "\nSaissez le niveau : \n");

            doc.append("name", nom)
                    .append("age", age)
                    .append("sexe", sexe)
                    .append("coordonneesGPS", coord)
                    .append("niveau", niv);
            currentCollectionName.insertOne(doc);

            System.out.println("\nL'insertion s'est correctement déroulée.\n");
            displayMenu();
        } else {
            System.out.print("Cette fonctionnalité n'est utilisable que sur la collection contactsReduits \n");
            displayMenu();
        }

    }

    private static void supprDB() {
        MongoCursor<String> dbsCursor = mongoClient.listDatabaseNames().iterator();
        Map<Integer, String> allDbs = new HashMap<Integer, String>();
        ArrayList<Integer> choix = new ArrayList<Integer>();
        System.out.println("\nBase de données disponibles :");
        int number = 1;
        while (dbsCursor.hasNext()) {
            choix.add(new Integer(number));
            String name = dbsCursor.next().toString();
            System.out.println(number + " : " + name);
            allDbs.put(number, name);
            number++;
        }

        Scanner scanner = new Scanner(System.in);
        Integer key = cliUtils.saisirEntier(scanner, "Choix: \n", choix);
        System.out.println();
        if (!allDbs.get(key).isEmpty()) {
            mongoClient.dropDatabase(allDbs.get(key));
            System.out.print("\nLa base de données " + allDbs.get(key) + " a bien été supprimée\n");
            displayMenu();
        } else {
            System.out.println("Cette base n'existe pas.\n");
            displayDatabases();
        }
    }

    private static void countDocuments() {
        checkDatabase();
        checkCollection();
        System.out.print("\nNombre de documents dans la collection " + currentCollectionName.getNamespace() + " : ");
        System.out.println(currentCollectionName.countDocuments() + "\n");

        displayMenu();
    }


    private static void checkDatabase() {
        if (currentDatabaseName == null) {
            System.out.println("Vous devez choisir une base de de données d'abord.\n");
            displayMenu();
        }
    }
}
