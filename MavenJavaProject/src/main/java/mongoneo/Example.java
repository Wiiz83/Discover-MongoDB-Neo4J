/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mongoneo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import java.util.Arrays;
import java.util.List;
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
import org.neo4j.driver.v1.Value;

/**
 *
 * @author user
 */
public class Example {

    /**
     * URI pour se connecter à Neo4J
     */
    public final static String NEO4J_DB_URI = "bolt://192.168.56.50";

    /**
     * URI pour se connecter à MongoDB
     */
    public final static String MONGO_DB_URI = "mongodb://localhost:27017";

    /**
     * Base de données MongoDB
     */
    public final static String MONGO_DB_DATABASE = "dbDocuments";

    /**
     * Collection MongoDB contenant les mots-clés
     */
    public final static String MONGO_DB_COLLECTION_INDEX = "index";

    /**
     * Collection MongoDB contenant les mots-clés inverse
     */
    public final static String MONGO_DB_COLLECTION_MIROIR = "indexInverse";

    /**
     * Caractère pour quitter le programme dans le menu
     */
    public final static String EXIT_CHAR = "0";

    /**
     * Réactive la couleur par défaut de la console
     */
    public static final String RESET = "\033[0m";

    /**
     * Affiche un message sur la console en ROUGE
     */
    public static final String RED = "\033[0;31m";

    /**
     * Affiche un message sur la console en VERT
     */
    public static final String GREEN = "\033[0;32m";

    /**
     * Clavier pour interrargir avec l'utilisateur sur la console
     */
    public final static Scanner clavier = new Scanner(System.in);

    /**
     * Affiche le menu pour sélectionner les différentes actions
     */
    public static final void afficherMenu() {
        System.out.println("\nMENU : ");
        System.out.println("\t1 - Mettre en place la structure des mots-clés en MongoDB");
        System.out.println("\t2 - Mettre en place la structure miroir en MongoDB");
        System.out.println("\t3 - Recherche tous les articles contenant un mot-clé");
        System.out.println("\t4 - Recherche tous les articles à partir d'une liste de mots-clés");
        System.out.println("\t5 - Afficher les 10 auteurs qui ont écrit le plus d'articles");
        System.out.println("\t" + EXIT_CHAR + " - Quitter le programme");
    }

    /**
     * Affiche un message sur la console et demande à l'utilisateur d'y
     * répondre.
     *
     * @param message message affiché sur la console
     * @return la réponse de l'utilisateur
     */
    public static String getReponseUtilisateur(String message) {
        System.out.print(message);
        return clavier.nextLine();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        boolean hasContinue = true;     // Permet de stopper le programme
        String reponse; // Réponse de l'utilisateur

        Session session = null; // Session pour se connecter à Neo4J

        // Supprime les messages de logs envoyés par MongoDB
        Logger logger = (Logger) Logger.getLogger("org.mongodb.driver");
        logger.setLevel(Level.OFF);

        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(MONGO_DB_URI));
                Driver neo4jClient = GraphDatabase.driver(NEO4J_DB_URI)) {
            while (hasContinue) {
                afficherMenu();
                reponse = getReponseUtilisateur("Veuillez sélectionner une action : ");
                System.out.println();
                session = neo4jClient.session();
                switch (reponse) {
                    case "1":
                        createDataBase(mongoClient, session);
                        break;
                    case "2":
                        createStructureMiroir(mongoClient);
                        break;
                    case "3":
                        rechercheDocument(mongoClient, session);
                        break;
                    case "4":
                        rechercheListesMotsCles(mongoClient, session);
                        break;
                    case "5":
                        rechercheAuteursPlusProductifs(session);
                        break;
                    case EXIT_CHAR:
                        hasContinue = false;
                        break;
                }
                session.close();
            }
        } catch (Exception exc) {
            /* Auto-closeable */
            System.err.println(exc.getMessage());
        } finally {
            if (session != null && session.isOpen()) {
                session.close();
            }
            clavier.close();
        }
    }

    /**
     * <p>
     * Mettre en place un datastore MongoDB. <br>
     * On récupère les articles disponibles dans Neo4J et pour chaque article,
     * on récupère les mots-clés du titre</p>
     *
     * @param mongoClient le client pour accéder au service MongoDB
     * @param session le client pour accéder au service Neo4J
     */
    private static void createDataBase(MongoClient mongoClient, Session session) {
        String reponse;             // Réponse de l'utilisateur
        boolean isCreate = true;    // True si l'on créé la BD MongoDB, false sinon

        /* Neo4J */
        StatementResult resultatRqt;    // La requête pour Neo4J
        Record rec;                     // Un enregistrement de la requête ci-dessus
        StringTokenizer token;          // Le titre éclaté en fonction des caractères séparateurs
        String[] motsCles;              // Les mots clés du titre de l'article

        /* MongoBD */
        MongoDatabase mongoDB;                      // La base de données MongoDB
        MongoCollection<Document> mongoCollection;  // La collection MongoDB
        Document document;                          // Le document que l'on ajoute

        if (isCollectionExist(mongoClient, MONGO_DB_DATABASE, MONGO_DB_COLLECTION_INDEX)) {
            System.out.println(RED + "La collection MongoDB '" + MONGO_DB_COLLECTION_INDEX + "' existe déjà !" + RESET);
            reponse = getReponseUtilisateur("Voulez-vous supprimer la collection existante (Y/N) ? ");
            isCreate = reponse.toUpperCase().equals("Y");
        }

        if (isCreate) {
            System.out.println("Mise à jour des mots-clés. Veuillez patienter.");
            resultatRqt = session.run("MATCH (a :Article) RETURN a.titre AS titre, ID(a) AS id");

            if (!resultatRqt.hasNext()) {
                System.out.println(RED + "Aucun article existe dans Neo4J !" + RESET);
            } else {
                mongoDB = mongoClient.getDatabase(MONGO_DB_DATABASE);
                mongoDB.drop();
                mongoCollection = mongoDB.getCollection(MONGO_DB_COLLECTION_INDEX);
                while (resultatRqt.hasNext()) {
                    rec = resultatRqt.next();
                    if (!rec.get("titre").isNull() && !rec.get("id").isNull()) {
                        token = new StringTokenizer(rec.get("titre").asString().toLowerCase(),
                                " ,-:;.()+[]{}?'");
                        motsCles = tokenToArray(token);
                        document = Document.parse("{idDocument : " + rec.get("id").asInt()
                                + ",motsCles : " + Arrays.toString(motsCles) + "}");
                        mongoCollection.insertOne(document);
                    }
                }
                System.out.println(GREEN + "Les motés-clés ont été mises à jour !" + RESET);
                System.out.println(mongoCollection.count() + " documents ont été ajoutés !");
                mongoCollection.createIndex(Document.parse("{motsCles : 1}"));
                System.out.println(GREEN + "Index créé sur les mots clés !" + RESET);
            }
        }
    }

    /**
     * <p>
     * Mettre en place une structure miroir sur MongoDB </p>
     *
     * @param mongoClient le client pour accéder au service MongoDB
     */
    private static void createStructureMiroir(MongoClient mongoClient) {
        String reponse;             // Réponse de l'utilisateur
        boolean isCreate = true;    // True si l'on créé la BD MongoDB, false sinon

        /* MongoBD */
        MongoDatabase mongoDB;                  // La base de données MongoDB
        MongoCollection<Document> mongoIndex, // La collection MongoDB de base
                mongoMiroir;                    // La collection MongoDB miroir

        if (isCollectionExist(mongoClient, MONGO_DB_DATABASE, MONGO_DB_COLLECTION_MIROIR)) {
            System.out.println(RED + "La Collection MongoDB '" + MONGO_DB_COLLECTION_MIROIR + "' existe déjà !" + RESET);
            reponse = getReponseUtilisateur("Voulez-vous supprimer la collection existante (Y/N) ? ");
            isCreate = reponse.toUpperCase().equals("Y");
        }

        if (isCreate) {
            System.out.println("Mise à jour de la structure miroir. Veuillez patienter.");
            mongoDB = mongoClient.getDatabase(MONGO_DB_DATABASE);
            mongoIndex = mongoDB.getCollection(MONGO_DB_COLLECTION_INDEX);
            mongoMiroir = mongoDB.getCollection(MONGO_DB_COLLECTION_MIROIR);
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

            System.out.println(GREEN + "La structure miroir a été mise à jour !" + RESET);
            System.out.println(mongoMiroir.count() + " documents ont été ajoutés !");
            mongoMiroir.createIndex(Document.parse("{documents : 1}"));
            System.out.println(GREEN + "Index créé sur la structure miroir !" + RESET);
        }
    }

    /**
     * Recherche les 10 auteurs qui ont écrit le plus d'articles
     *
     * @param session le client pour accéder à Neo4J
     */
    private static void rechercheAuteursPlusProductifs(Session session) {
        StatementResult resultatRqt;    // La requête pour Neo4J
        Record rec;                     // Un enregistrement de la requête ci-dessus

        resultatRqt = session.run("MATCH (au :Auteur)-[e :Ecrire]->(:Article)\n"
                + "RETURN au.nom AS nom, count(e) AS nb_articles\n"
                + "ORDER BY nb_articles DESC, nom ASC\n"
                + "LIMIT 10");

        if (!resultatRqt.hasNext()) {
            System.out.println(RED + "Aucun auteur n'est présent dans la BD !" + RESET);
        } else {
            System.out.println("Liste des 10 auteurs qui ont écrit le plus d'articles :");
            while (resultatRqt.hasNext()) {
                rec = resultatRqt.next();
                if (!rec.get("nom").isNull() && !rec.get("nb_articles").isNull()) {
                    System.out.println("\t" + rec.get("nb_articles").asLong()
                            + " - " + rec.get("nom").asString());
                }
            }
        }
    }

    /**
     * Recherche un mot-clé dans la structure miroir de MongoDB et on affiche
     * les titres des documents qui contiennent ce mot-clé
     *
     * @param mongoClient le client pour accéder au service MongoDB
     * @param session le client pour accéder au service Neo4J
     */
    private static void rechercheDocument(MongoClient mongoClient, Session session) {
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

        if (!isCollectionExist(mongoClient, MONGO_DB_DATABASE, MONGO_DB_COLLECTION_MIROIR)) {
            System.out.println(RED + "La structure miroir n'a pas été créé !" + RESET);
        } else {
            motCle = getReponseUtilisateur("Quel mot-clé souhaitez vous rechercher ? ");
            mongoDB = mongoClient.getDatabase(MONGO_DB_DATABASE);
            mongoMiroir = mongoDB.getCollection(MONGO_DB_COLLECTION_MIROIR);
            document = mongoMiroir.find(Filters.eq("mot", motCle)).first();

            if (document == null) {
                System.out.println(RED + "Le mot-clé '" + motCle + "' n'existe pas dans la base !" + RESET);
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

    /**
     * Recherche une liste de mots-clés dans la structure miroir de MongoDB et
     * on affiche les titres des documents qui contiennent ces mots-clés
     *
     * @param mongoClient le client pour accéder au service MongoDB
     * @param session le client pour accéder au service Neo4J
     */
    private static void rechercheListesMotsCles(MongoClient mongoClient, Session session) {
        String reponse;             // Les mots-clés que l'utilisateur souhaite rechercher
        String[] motsCles;          // La liste des mots-clés

        /* Neo4J */
        StatementResult resultatRqt;    // La requête pour Neo4J
        Value titre;                    // Le titre d'un article qui contient le mot clé

        /* MongoBD */
        MongoDatabase mongoDB;                  // La base de données MongoDB
        MongoCollection<Document> mongoMiroir;  // La collection MongoDB
        AggregateIterable<Document> documents;       // Les documents MongoDB correspondant aux mots-clés

        if (!isCollectionExist(mongoClient, MONGO_DB_DATABASE, MONGO_DB_COLLECTION_MIROIR)) {
            System.out.println(RED + "La structure miroir n'a pas été créé !" + RESET);
        } else {
            reponse = getReponseUtilisateur("Quel listes de mots-clés souhaitez vous rechercher (séparer les mots-clés par une ',') ?\n");
            motsCles = tokenToArray(new StringTokenizer(reponse, ","));
            System.out.println("Liste des mots-clés recherchés : " + Arrays.toString(motsCles));

            mongoDB = mongoClient.getDatabase(MONGO_DB_DATABASE);
            mongoMiroir = mongoDB.getCollection(MONGO_DB_COLLECTION_MIROIR);

            /* Requête équivalente à : 
             * db.indexInverse.aggregate([
             * { $match : {
             *         mot : {$in : [<ma_liste_de_mots_clés>]}
             *     } 
             * },
             * { $unwind : "$documents" },
             * { $group : {
             *         _id : "$documents",
             *         nb_correspondances : { $sum : 1 }
             *     }
             * },
             * { $sort : {nb_correspondances : -1} },
             * { $limit : 10 }])
             */
            documents = mongoMiroir.aggregate(Arrays.asList(
                    Document.parse("{ $match : {\n" +
                                            "mot : {$in : " + Arrays.toString(motsCles) + "}\n" +
                                        "}\n" +
                                    "}"),
                    Document.parse("{ $unwind : \"$documents\" }"),
                    Document.parse("{ $group : {\n" +
                                            "_id : \"$documents\",\n" +
                                            "nb_correspondances : { $sum : 1 }\n" +
                                        "}\n" +
                                    "}"),
                    Document.parse("{ $sort : {nb_correspondances : -1} }"),
                    Document.parse("{ $limit : 10 }")));
            if (documents == null) {
                System.out.println(RED + "Aucun article ne correspond à la liste de mots-clés !" + RESET);
            } else {
                for (Document d : documents) {
                    resultatRqt = session.run("MATCH (a :Article) \n"
                            + "WHERE ID(a) = " + d.get("_id") + "\n"
                            + "RETURN a.titre AS titre");
                    titre = resultatRqt.next().get("titre");
                    if (!titre.isNull()) {
                        System.out.println(d.get("nb_correspondances") + " mots-clés en commun\t- " + titre.asString());
                    }
                }
            }
        }
    }
    
    /**
     * <p>
     * Vérifie si la collection MongoBD existe déjà </p>
     *
     * @param mongoClient le client pour accéder au service MongoDB
     * @param nomBD le nom de la base de données MongoDB
     * @param nomCollection le nom de la collection MongoDB
     *
     * @return true si la collection existe, false sinon
     */
    private static boolean isCollectionExist(MongoClient mongoClient,
            String nomBD, String nomCollection) {
        for (String bdName : mongoClient.listDatabaseNames()) {
            if (bdName.equals(nomBD)) { // On a reconnu la base de données dans MongoDB
                for (String collectionName : mongoClient.getDatabase(nomBD).listCollectionNames()) {
                    if (collectionName.equals(nomCollection)) { // On a reconnu la collection dans MongoDB
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Transforme une StringTokenizer en un tableau de chaîne
     *
     * @param token la StringTokenizer
     * @return un tableau de String
     */
    private static String[] tokenToArray(StringTokenizer token) {
        String[] array = new String[token.countTokens()];
        for (int i = 0, n = array.length; i < n; i++) {
            array[i] = "\"" + token.nextToken().trim() + "\"";
        }
        return array;
    }
}

