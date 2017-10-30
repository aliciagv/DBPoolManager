/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cice.dbpoolmanager;

import com.mysql.jdbc.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;

/**
 *
 * @author cice
 */
public class DBPoolManager {
    
    private static final String DRIVER="com.mysql.jdbc.Driver";
    private String host, port, user, pass, database, url;
    
    DataSource dataSource;

    public DBPoolManager() {
    }

    public DBPoolManager(String host, String port, String user, String pass, String database, String url, DataSource dataSource) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.pass = pass;
        this.database = database;
        this.url = url;
        
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName(DRIVER);
        basicDataSource.setUsername(user);
        basicDataSource.setPassword(pass);
        basicDataSource.setUrl("jdbc:mysql://"+host+":"+port+ "/"+database);

        // Opcional. Sentencia SQL que le puede servir a BasicDataSource
        // para comprobar que la conexion es correcta.
        basicDataSource.setValidationQuery("select 1");
     
        this.dataSource = dataSource;
    }
    
    private Connection dbConnect() throws Exception {
        Connection connection;
        try {
            connection = (Connection) dataSource.getConnection();
        } catch (SQLException ex) {
              throw new Exception("Se ha producido un error al conectar con la DB",ex);
        }
        return  connection;
    
    }
    
     public void executeUpdate(String sql){
        
        try {
            Connection connection=dbConnect();
            
            Statement st = connection.createStatement();
            st.executeUpdate(sql);
            
            st.close();
            dbCloseConnection(connection);
        } catch (Exception ex) {
             Logger.getLogger(DBPoolManager.class.getCanonicalName()).log(Level.SEVERE, ex.getLocalizedMessage(),ex);
        }
     }
    
     public void dbCloseConnection( Connection connection) throws Exception{
        try {
            connection.close();
        } catch (SQLException ex) {
             throw new Exception("Se ha producido un error al desconectar con la DB",ex);
        }
    
    }
     
     public ResultSet executeSelect(String sql){
     
         ResultSet busqueda=null;
         
        try {
            
            Connection connection=dbConnect();
            Statement st = connection.createStatement();
            busqueda = st.executeQuery(sql);
            st.close();
            dbCloseConnection(connection);
            
        } catch (Exception ex) {
            Logger.getLogger(DBPoolManager.class.getName()).log(Level.SEVERE, null, ex);
        }
         
         return busqueda;
     }
    
}
