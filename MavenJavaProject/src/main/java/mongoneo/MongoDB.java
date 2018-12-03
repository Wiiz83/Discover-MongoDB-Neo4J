/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mongoneo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 *
 * @author uzanl
 */
public class MongoDB {
    
    private MongoDatabase currentDatabaseName;
    private MongoCollection<Document> currentCollectionName;
    private MongoClient mongoClient;
    
    public MongoDB(){
        mongoClient = new MongoClient(new MongoClientURI("mongodb://127.0.0.1:27017"));
    }
    
    public void getDatabase(String nom) throws Exception {
        mongoClient.getDatabase(nom);
    }
    
    public void deleteDatabase(String nom) throws Exception {
        mongoClient.dropDatabase(nom);
    }
    
    public void closeConnection(){
        mongoClient.close();
    }
    
}
