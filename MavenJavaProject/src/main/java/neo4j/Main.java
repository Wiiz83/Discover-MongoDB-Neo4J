package neo4j;

import utils.CLIUtils;
import java.util.ArrayList;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import java.util.Scanner;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;

public class Main {

    private static final CLIUtils cliUtils = new CLIUtils();
    private static Driver driver;
    private static int compteur = 0;

    public static void main(String[] args) {
        // création de la configuration de connexion à la base neo4j
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "Nx2yepGD"));

        // création d'une session de connexion à la base neo4j
        try (Session session = driver.session()) {
            // si réussie, on affiche le menu
            System.out.println("Connexion à la base réussie.");
        } catch (Exception e) {
            // sinon on quitte l'application
            System.out.println("Connexion à la base impossible : " + e.getMessage());
            closeApplication();
        }
        displayMenu();
    }

    private static void displayMenu() {
        compteur = 0;
        System.out.println("- - - - - - - - - - - - - - - - - - - - -");
        System.out.println("(1) Lister les films disponibles \n"
                + "(2) Lister les personnes disponibles \n"
                + "(3) Afficher les 3 films les mieux notés \n"
                + "(4) Afficher maximum 5 films ʹprochesʹ \n"
                + "(5) Afficher la meilleure note attribuée à un film \n"
                + "(6) Afficher les personnes qui ont participé à la création d'un film \n"
                + "(0) Quitter lʹapplication");
        System.out.println("- - - - - - - - - - - - - - - - - - - - -");
        Scanner scanner = new Scanner(System.in);
        ArrayList<Integer> choix = new ArrayList<Integer>() {
            {
                add(new Integer(0));
                add(new Integer(1));
                add(new Integer(2));
                add(new Integer(3));
                add(new Integer(4));
                add(new Integer(5));
                add(new Integer(6));
            }
        };
        Integer c = cliUtils.saisirEntier(scanner, "Choix: ", choix);
        switch (c) {
            case 0:
                closeApplication();
                break;
            case 1:
                displayMovies();
                break;
            case 2:
                displayPersons();
                break;
            case 3:
                display3BestRatedMovies();
                break;
            case 4:
                display5CloseMovies();
                break;
            case 5:
                displayBestMovieRate();
                break;
            case 6:
                displayMovieParticipants();
                break;
        }
    }

    private static void closeApplication() {
        // on ferme le driver de connexion à la base
        driver.close();
        System.out.println("Merci de votre visite !");
        // on quitte l'application 
        System.exit(0);
    }

    private static void displayMovies() {
        // création et exécution de la session 
        try (Session session = driver.session()) {
            // création et exécution de la requête 
            StatementResult result = session.run("MATCH (n:Movie) RETURN n.title, n.released, n.tagline ORDER BY n.released DESC");
            System.out.println("Films disponibles :");
            Record record;
            // affichage des résultats 
            while (result.hasNext()) {
                record = result.next();
                compteur++;
                if (compteur > 5) {
                    continuer();
                }
                System.out.println(record.get("n.title").asString() + " - " + record.get("n.released") + "(" + record.get("n.tagline").asString() + ")");
            }
        }
        // retour au menu
        displayMenu();
    }

    private static void displayPersons() {
        try (Session session = driver.session()) {
            StatementResult result = session.run("match (p:Person)-[r]-(m:Movie) return p.name, p.born, m.title, type (r) ORDER BY p.name");
            System.out.println("Personnes disponibles :");
            Record record;
            String personneEnCours = "";
            while (result.hasNext()) {
                record = result.next();
                compteur++;
                if (compteur > 5) {
                    continuer();
                }
                if (!personneEnCours.equals(record.get("p.name").asString())) {
                    //première fois qu'on voit la personne
                    personneEnCours = record.get("p.name").asString();
                    System.out.println(record.get("p.name").asString() + " (" + record.get("p.born").toString() + ") ");
                }
                System.out.println("\t" + record.get("type (r)").asString() + "[" + record.get("m.title") + "]");
            }
        }
        displayMenu();
    }

    private static void display3BestRatedMovies() {
        // création et exécution de la session 
        try (Session session = driver.session()) {
            // création et exécution de la requête 
            StatementResult result = session.run("MATCH (:Person)-[r:REVIEWED]->(m:Movie) RETURN r.rating, m.title ORDER BY r.rating DESC LIMIT 3");
            System.out.println("3 films les mieux notés :");
            Record record;
            // affichage des résultats 
            while (result.hasNext()) {
                record = result.next();
                compteur++;
                if (compteur > 5) {
                    continuer();
                }
                System.out.println(record.get("m.title").asString() + " - " + record.get("r.rating"));
            }
        }
        // retour au menu
        displayMenu();
    }

    private static void display5CloseMovies() {
        Scanner scanner = new Scanner(System.in);
        String titre = cliUtils.saisirChaine(scanner, "Donner le titre du film : \n");

        try (Session session = driver.session()) {
            // création et exécution de la requête 
            StatementResult result = session.run(""
                    + "match (m1:Movie)<-[a1:ACTED_IN]-(p:Person)-[a2:ACTED_IN]->(m2:Movie) "
                    + "where m1.title = '" + titre + "' "
                    + "return m1.title,m2.title,count(p) as nb "
                    + "order by nb desc,m2.title asc limit 5");
            System.out.println("Au plus 5 films proche de " + titre + " :");
            Record record;
            // affichage des résultats 
            while (result.hasNext()) {
                record = result.next();
                compteur++;
                if (compteur > 5) {
                    continuer();
                }
                System.out.println(record.get("m2.title").asString() + " - " + record.get("nb"));
            }
        } catch (Exception e) {
            System.out.println("Le film " + titre + " n'existe pas dans la base.");
        }
        displayMenu();
    }

    private static void displayBestMovieRate() {
        // récupérer le titre du film
        Scanner scanner = new Scanner(System.in);
        String titre = cliUtils.saisirChaine(scanner, "Donner le titre du film : \n");

        // création et exécution de la session 
        try (Session session = driver.session()) {
            StatementResult result = session.run("MATCH (p:Person)-[r:REVIEWED]->(m:Movie) WHERE m.title = '" + titre + "' RETURN max(r.rating) as bestRate");
            Record record = result.next();
            compteur++;
            if (compteur > 5) {
                continuer();
            }
            if (!record.get("bestRate").isNull()) {
                System.out.println("La note maximale est " + record.get("bestRate") + " pour le fim " + titre);
            } else {
                System.out.println("Ce film n'a pas de note.");
            }

        } catch (Exception e) {
            System.out.println("Le film " + titre + " n'existe pas dans la base.");
        }
        displayMenu();
    }

    private static void displayMovieParticipants() {
        // récupérer le titre du film
        Scanner scanner = new Scanner(System.in);
        String titre = cliUtils.saisirChaine(scanner, "Donner le titre du film : \n");

        // création et exécution de la session 
        try (Session session = driver.session()) {
            StatementResult result = session.run("MATCH (p:Person)-[r]->(m:Movie) WHERE m.title = '" + titre + "' RETURN p.name, type(r) as type");
            Record record;
            System.out.println("Les participants à la création du film " + titre + " :");
            while (result.hasNext()) {
                record = result.next();
                compteur++;
                if (compteur > 5) {
                    continuer();
                }
                System.out.println(record.get("p.name").asString() + " : " + record.get("type"));
            }
        } catch (Exception e) {
            System.out.println("Le film " + titre + " n'existe pas dans la base.");
        }
        displayMenu();
    }

    private static void continuer() {
        //récupérer le OUI de l'utilisateur
        Scanner scanner = new Scanner(System.in);
        boolean continuer = cliUtils.yesNoQuestion(scanner, "Afficher les 5 résultats suivants ? (y/n) ");
        if (continuer) {
            compteur = 0;
        } else {
            displayMenu();
        }
    }

}
