package mongoneo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import utils.CLIUtils;

public class Main {
    
    private static CLIUtils cliUtils;
    private static Driver driver;
    private static MongoClient mongoClient;

    public static void main(String[] args) {
        try {
            // pour faciliter la création de scanners utilisateurs 
            cliUtils = new CLIUtils();
            // création de la connexion à la base MongoDB
            mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
            // création de la connexion à la base Neo4J
            driver = GraphDatabase.driver("bolt://localhost:7687");
            // affichage du menu 
            displayMenu();
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void displayMenu() throws Exception {
        System.out.println(
                "1) Mettre en place un datastore MongoDB \n"
                + "2) Mettre en place un index sur le tableau de motsClés dans MongoDB \n"
                + "3) Mise en place d’une « structure miroir » sur MongoDB \n"
                + "4) Recherche de documents \n"
                + "5) Auteurs ayant écrit le plus d’articles \n"
                + "6) Recherche de documents avancée \n"
                + "7) Quitter l'application"
        );
        Scanner scanner = new Scanner(System.in);
        ArrayList<Integer> choix = new ArrayList<Integer>() {
            {
                add(1);
                add(2);
                add(3);
                add(4);
                add(5);
                add(6);
                add(7);
            }
        };
        Integer c = cliUtils.saisirEntier(scanner, "Choix: \n", choix);
        switch (c) {
            case 1:
                creerDatastoreMongoDB();
                break;
            case 2:
                creerIndexMotsClesMongoDB();
                break;
            case 3:
                creerStructureMiroir();
                break;
            case 4:
                rechercheDocuments();
                break;
            case 5:
                rechercheAuteursDePlusArticles();
                break;
            case 6:
                rechercheDocumentsAvancee();
                break;
            case 7:
                quitterApplication();
                break;
        }
    }

    public static void creerDatastoreMongoDB() throws Exception {
        // récupération ou création de la base et de la collection appropriées 
        MongoDatabase mongoDatabase = mongoClient.getDatabase("dbDocuments");
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("index");

        // pour chaque article dans la base neo4j, on créé et on insère un document dans la collection mongodb
        try (Session session = driver.session()) {
            StatementResult resultat = session.run("MATCH (a:Article) RETURN a.titre AS titre, ID(a) AS id");
            Record record;
            while (resultat.hasNext()) {
                record = resultat.next();
                if (!record.get("titre").isNull() && !record.get("id").isNull()) {
                    StringTokenizer token = new StringTokenizer(record.get("titre").asString().toLowerCase(), " ,-:;.()+[]{}?'");
                    String[] motsCles = new String[token.countTokens()];
                    for (int i = 0, n = motsCles.length; i < n; i++) {
                        motsCles[i] = "\"" + token.nextToken().trim() + "\"";
                    }
                    Document document = Document.parse("{idDocument : "+record.get("id").asInt()+",motsCles : "+Arrays.toString(motsCles)+"}");
                    mongoCollection.insertOne(document);
                }
            }
        }
    }

    public static void creerIndexMotsClesMongoDB() {
        // récupération ou création de la base et de la collection appropriées 
        MongoDatabase mongoDatabase = mongoClient.getDatabase("dbDocuments");
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("index");
        
        // mise en place de l'index (ensureIndex) sur le tableau nommé motClés dans MongoDB
        mongoCollection.createIndex(Document.parse("{motsCles : 1}"));
    }

    public static void creerStructureMiroir() {
        String reponse;             // Réponse de l'utilisateur
        boolean isCreate = true;    // True si l'on créé la BD MongoDB, false sinon

        /* MongoBD */
        MongoDatabase mongoDB;                  // La base de données MongoDB
        MongoCollection<Document> mongoIndex, // La collection MongoDB de base
                mongoMiroir;                    // La collection MongoDB miroir

        if (isCollectionExist(mongoClient, "dbDocuments", "indexInverse")) {
            System.out.println(RED + "La Collection MongoDB 'indexInverse' existe déjà !");
            reponse = getReponseUtilisateur("Voulez-vous supprimer la collection existante (Y/N) ? ");
            isCreate = reponse.toUpperCase().equals("Y");
        }

        if (isCreate) {
            System.out.println("Mise à jour de la structure miroir. Veuillez patienter.");
            mongoDB = mongoClient.getDatabase("dbDocuments");
            mongoIndex = mongoDB.getCollection("index");
            mongoMiroir = mongoDB.getCollection("indexInverse");
            mongoMiroir.drop(); // On supprime uniquement la collection miroir

            for (Document doc : mongoIndex.find()) {
                for (String motCle : (List<String>) doc.get("motsCles")) {
                    Document d = mongoMiroir.find(Filters.eq("mot", motCle)).first();
                    if (d == null) {
                        mongoMiroir.insertOne(Document.parse("{mot: \"" + motCle
                                + "\", documents : [" + doc.getInteger("idDocument") + "]}"));
                    } else {
                        mongoMiroir.updateOne(Filters.eq("_id", d.get("_id")),
                                Updates.addToSet("documents", doc.getInteger("idDocument")));
                    }
                }
            }

            System.out.println("La structure miroir a été mise à jour !");
            System.out.println(mongoMiroir.count() + " documents ont été ajoutés !");
            mongoMiroir.createIndex(Document.parse("{documents : 1}"));
            System.out.println("Index créé sur la structure miroir !");
        }

    }

    private static void rechercheDocuments() {
        String motCle;  // Mot clé que l'on doit rechercher

        /* Neo4J */
        StatementResult resultatRqt;    // La requête pour Neo4J
        List<Record> records;           // Les résultats de la requête Neo4j
        Value titre;                    // Le titre d'un article qui contient le mot clé

        /* MongoBD */
        MongoDatabase mongoDB;                  // La base de données MongoDB
        MongoCollection<Document> mongoMiroir;  // La collection MongoDB
        Document document;                      // Le document que l'on ajoute
        List<Integer> listDocuments;            // Liste des documents qui contiennent le mot clé

        if (!isCollectionExist(mongoClient, "dbDocuments", "indexInverse")) {
            System.out.println("Il faut d'abord créer une structure miroir");
        } else {
            motCle = getReponseUtilisateur("Mot-clé à rechercher ? ");
            mongoDB = mongoClient.getDatabase("dbDocuments");
            mongoMiroir = mongoDB.getCollection("indexInverse");
            document = mongoMiroir.find(Filters.eq("mot", motCle)).first();

            if (document == null) {
                System.out.println("Le mot-clé '" + motCle + "' n'existe pas dans la base !");
            } else {
                listDocuments = (List<Integer>) document.get("documents");
                resultatRqt = session.run("MATCH (a :Article) \n"
                        + "WHERE ID(a) IN " + Arrays.toString(listDocuments.toArray()) + "\n"
                        + "RETURN a.titre AS titre\n"
                        + "ORDER BY titre ASC");
                records = resultatRqt.list();
                System.out.println("Liste des titres en lien avec le mot-clé '" + motCle + "' : " + records.size() + " articles");
                for (Record record : records) {
                    titre = record.get("titre");
                    if (!titre.isNull()) {
                        System.out.println("\t- " + titre.asString());
                    }
                }
            }
        }
    }

    public static void rechercheAuteursDePlusArticles() {
        // récupérer les 10 auteurs ayant écrit le plus d’articles
        try (Session session = driver.session()) {
            StatementResult result = session.run("MATCH (au:Auteur)-[e:Ecrire]->(a:Article) RETURN au.nom AS nom, count(e) AS nb_articles ORDER BY nb_articles DESC, nom ASC LIMIT 10");
            Record record;
            while (result.hasNext()) {
                record = result.next();
                if (!record.get("nom").isNull() && !record.get("nb_articles").isNull()) {
                    System.out.println("\t" + record.get("nb_articles").asLong() + " - " + record.get("nom").asString());
                }
            }
        }
    }

    public static void rechercheDocumentsAvancee() throws Exception {
        // récupération des mots clés à rechercher 
        Scanner scanner = new Scanner(System.in);
        String s = cliUtils.saisirChaine(scanner, "Quels sont les mot-clés que vous souhaitez rechercher ? Exemple : 'with, systems, new'");
        StringTokenizer token = new StringTokenizer(s, ",");
        String[] motsCles = new String[token.countTokens()];
        for (int i = 0, n = motsCles.length; i < n; i++) {
            motsCles[i] = "\"" + token.nextToken().trim() + "\"";
        }

        // rechercher les 10 premiers documents sur la structure miroir MongoDB qui possèdent les mot-clés 
        MongoDatabase mongoDatabase = mongoClient.getDatabase("dbDocuments");
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("indexInverse");
        AggregateIterable<Document> documents = mongoCollection.aggregate(Arrays.asList(
                Document.parse("{$match:{mot:{$in:" + Arrays.toString(motsCles) + "}}}"),
                Document.parse("{$unwind:\"$documents\"}"),
                Document.parse("{$group:{_id :\"$documents\",nb_correspondances:{$sum:1}}}"),
                Document.parse("{$sort:{nb_correspondances:-1}}"),
                Document.parse("{$limit:10}")));

        // pour chaque documents, récupérer le titre de l'article correspondant au document en cours 
        StatementResult result;
        try (Session session = driver.session()) {
            for (Document d : documents) {
                result = session.run("MATCH (a:Article) WHERE ID(a) = "+d.get("_id")+"RETURN a.titre AS titre");
                if (!result.next().get("titre").isNull()) {
                    System.out.println(d.get("nb_correspondances") + " mots-clés en commun\t- " + result.next().get("titre").asString());
                }
            }
        }
    }

    public static void quitterApplication() {
        mongoClient.close();
        driver.close();
        System.exit(0);
    }

}
