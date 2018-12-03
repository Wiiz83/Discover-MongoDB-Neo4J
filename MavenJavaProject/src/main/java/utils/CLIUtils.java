package utils;

import java.util.ArrayList;
import java.util.Scanner;

public class CLIUtils {

    public Integer saisirEntier(Scanner sc, String label, ArrayList<Integer> range) {
        do {
            try {
                System.out.print(label);
                Integer d = Integer.parseInt(sc.next());
                if (!range.contains(d)) {
                    throw new NumberFormatException("nombre incorrect");
                }
                return d;
            } catch (NumberFormatException ex) {
                System.out.println("Erreur de saisie : " + ex.getMessage());
            }
        } while (true);
    }

    public Long saisirEntier(Scanner sc, String label, Long limiteInf, Long limiteMax) {
        do {
            try {
                System.out.print(label);
                Long d = Long.parseLong(sc.next());
                if (d < limiteInf || d > limiteMax) {
                    throw new NumberFormatException("nombre incorrect");
                }
                return d;
            } catch (NumberFormatException ex) {
                System.out.println("Erreur de saisie : " + ex.getMessage());
            }
        } while (true);
    }

    public String saisirChaine(Scanner sc, String label) {
        do {
            System.out.print(label);
            String c = sc.nextLine();
            c = c == null ? null : c.trim();
            if (c == null || c.isEmpty()) {
                System.out.println("Erreur de sasisie : veuillez saisir au moins un caractère.");
            } else {
                return c;
            }
        } while (true);
    }

    public boolean yesNoQuestion(Scanner sc, String label) {
        do {
            String s = saisirChaine(sc, label).toLowerCase();
            switch (s) {
                case "y":
                    return true;
                case "n":
                    return false;
                default:
                    System.out.println("Erreur de saisie : veuillez saisir 'y' ou 'n'.");
            }
        } while (true);
    }

}