# TPMongoDBNeo4J

## Installation de MongoDB

Nous avons téléchargé puis installé la version “Community Server” sur nos PC en local. Nous avons également installé Robo 3T, un logiciel qui propose une interface graphique pour requêter notre base MongoDB (MongoDB Compass ne propose pas cette fonctionnalité). Lancement du serveur MongoDB avec la commande : C:\mongodb-win32-x86_64-2008plus-ssl-4.0.4\bin>mongod

Tutoriel d'installation : https://docs.mongodb.com/manual/tutorial/install-mongodb-on-windows/ 

Lien vers le téléchargement de MongoDB : https://www.mongodb.com/download-center/community 

Lien vers Roboto 3T : https://robomongo.org/ 


## Installation de Neo4j

Nous avons télécharger Neo4j version “Community Server”. Ensuite, nous avons lancé le serveur avec la commande “./neo4j.bat console”. Enfin, nous avons ouvert le browser Neo4j à l’URL http://localhost:7474/.

Documentation du driver neo4j : 
* https://neo4j.com/developer/java/
* https://neo4j.com/docs/api/java-driver/current/
* https://github.com/neo4j/neo4j-java-driver


## Configuratio projet IntelliJ

Note : ne pas importer le dossier .idea dans le projet 

Création projet java : 
* https://www.tutorialspoint.com/intellij_idea/intellij_idea_create_first_java_project.htm 

Ajout du driver dans le buid path du projet : 
* https://stackoverflow.com/questions/1051640/correct-way-to-add-external-jars-lib-jar-to-an-intellij-idea-project



## Erreurs repertoriées 

**Créer un répertoire /data/db dans le C:**

![erreur](https://raw.githubusercontent.com/Wiiz83/TPMongoDBNeo4J/master/Images/erreur-mongodb-datadirectory.PNG?token=AKc2Y8skxuiNS-yTQ8kPw2SdnFbzV555ks5cDh5jwA%3D%3D)

Source : https://stackoverflow.com/questions/8029064/new-to-mongodb-can-not-run-command-mongo
