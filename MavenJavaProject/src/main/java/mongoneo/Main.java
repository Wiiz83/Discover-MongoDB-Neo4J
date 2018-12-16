package mongoneo;

import com.mongodb.client.FindIterable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import utils.CLIUtils;

public class Main {
    
    private static MongoDB mongodb;
    private static Neo4J neo4j; 
    private static CLIUtils cliUtils;

    public static void main(String[] args) {
        try {
            cliUtils = new CLIUtils();
            mongodb = new MongoDB();
            neo4j = new Neo4J();
            displayMenu();
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }
    
    private static void displayMenu() throws Exception {
        System.out.println(
                "1) Mettre en place un datastore MongoDB \n" +
                "2) Mettre en place un index sur le tableau de motsClés dans MongoDB \n" +
                "3) Mise en place d’une « structure miroir » sur MongoDB \n" +
                "4) Recherche de documents \n" +
                "5) Auteurs ayant écrit le plus d’articles \n" +
                "6) Recherche de documents avancée \n" +
                "7) Quitter l'application"
        );
        Scanner scanner = new Scanner(System.in);
        ArrayList<Integer> choix = new ArrayList<Integer>() {{
            add(1);
            add(2);
            add(3);
            add(4);
            add(5);
            add(6);
            add(7);
        }};
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
            case 7 :
                quitterApplication();
                break;
        }
    }
    
   /*
    Sur la base des articles disponibles dans Neo4J, écrire une méthode permettant de
    construire une base de documents MongoDB qui va créer pour chaque article dans
    Neo4J un document ayant le format suivant dans une nouvelle collection nommée
    index
    */
    public static void creerDatastoreMongoDB() throws Exception{
        String NomBaseMongo = "dbDocuments";
        String NomCollectionMongo = "index";
        boolean exist = false;
        
        // vérifier si la base et la collection n'existe pas déjà
        for (String bdName : mongodb.mongoClient.listDatabaseNames()) {
            if (bdName.equals(NomBaseMongo)) { // On a reconnu la base de données dans MongoDB
                for (String collectionName : mongodb.mongoClient.getDatabase(NomBaseMongo).listCollectionNames()) {
                    if (collectionName.equals(NomCollectionMongo)) { // On a reconnu la collection dans MongoDB
                        exist = true;
                    }
                }
            }
        }

        // si la base et la collection n'existe pas déjà
        if(exist == true){
            /*
            System.out.println(RED + "La collection MongoDB '" + MONGO_DB_COLLECTION_INDEX + "' existe déjà !" + RESET);
            reponse = getReponseUtilisateur("Voulez-vous supprimer la collection existante (Y/N) ? ");
            isCreate = reponse.toUpperCase().equals("Y");
            */
        } 
        
        // si elle n'existe pas, on crée la base dbDocuments
        else {
            mongodb.getDatabase(NomBaseMongo);
            mongodb.currentCollectionName = mongodb.currentDatabaseName.getCollection(NomCollectionMongo);
            
            try (Session session = neo4j.driver.session()) {
                StatementResult resultatRqt = session.run("MATCH (a:Article) RETURN a.titre AS titre, ID(a) AS id");
                while (resultatRqt.hasNext()) {
                    Record rec = resultatRqt.next();
                    if (!rec.get("titre").isNull() && !rec.get("id").isNull()) {
                        StringTokenizer token = new StringTokenizer(rec.get("titre").asString().toLowerCase()," ,-:;.()+[]{}?'");
                        String[] motsCles = new String[token.countTokens()];
                        for (int i = 0, n = motsCles.length; i < n; i++) {
                            motsCles[i] = "\"" + token.nextToken().trim() + "\"";
                        }
                        Document document = Document.parse("{idDocument : "+rec.get("id").asInt()+",motsCles : "+Arrays.toString(motsCles)+"}");
                        mongodb.currentCollectionName.insertOne(document);
                    }
                }
            }
        }        
    }
    
    public static void creerIndexMotsClesMongoDB(){
        mongodb.currentCollectionName.createIndex(Document.parse("{motsCles : 1}"));
    }
    
    public static void creerStructureMiroir(){
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
    
    public static void rechercheAuteursDePlusArticles(){
        try (Session session = neo4j.driver.session()) {
            StatementResult result = session.run("MATCH (au :Auteur)-[e :Ecrire]->(:Article) RETURN au.nom AS nom, count(e) AS nb_articles ORDER BY nb_articles DESC, nom ASC LIMIT 10");
            Record record;
            while (result.hasNext()) {
                record = result.next();
                if (!record.get("nom").isNull() && !record.get("nb_articles").isNull()) {
                    System.out.println("\t"+record.get("nb_articles").asLong()+" - "+record.get("nom").asString());
                }
            }
        }
    }
    
    public static void rechercheDocumentsAvancee(){
        String NomBaseMongo = "dbDocuments";
        String NomCollectionMongo = "indexInverse";
        boolean exist = false;
        
        String reponse = getReponseUtilisateur("Quel listes de mots-clés souhaitez vous rechercher (séparer les mots-clés par une ',') ?\n");
        String[] motsCles = tokenToArray(new StringTokenizer(reponse, ","));
        System.out.println("Liste des mots-clés recherchés : " + Arrays.toString(motsCles));

        mongodb.getDatabase(nom);
        mongodb.currentCollectionName = mongodb.currentDatabaseName.getCollection(NomCollectionMongo);            
        FindIterable<Document> documents = mongodb.currentCollectionName.aggregate("{$match:{mot:{$in:"+Arrays.toString(motsCles)+"}}}"
                + "{$unwind:\"$documents\"}"
                + "{$group:{_id:\"$documents\",nb_correspondances:{$sum:1}}}"
                + "{$sort:{nb_correspondances:-1}}"
                + "{$limit:10}");

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
  
    
    public static void quitterApplication(){
        mongodb.closeConnection();
        neo4j.closeConnection();
        System.exit(0);
    }
    
}
