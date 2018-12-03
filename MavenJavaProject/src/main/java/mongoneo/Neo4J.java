/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mongoneo;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

/**
 *
 * @author uzanl
 */
public class Neo4J {
    
    private final Driver driver;
    
    public Neo4J(){
        driver = GraphDatabase.driver("bolt://localhost:7687");
    }
    
    public void closeConnection() {
        driver.close();
    }
    
    
}
