/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cice.dbpoolmanager;

import com.sun.rowset.CachedRowSetImpl;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import javax.sql.rowset.CachedRowSet;
import org.apache.commons.dbcp.BasicDataSource;

/**
 *
 * @author cice
 */
public class DBPoolManager {

    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private String host, port, user, pass, database, url;

    private int minInitialSize;
    private int maxActive;

    DataSource dataSource;
    BasicDataSource basicDataSource;

    public DBPoolManager() {
       
        
        this.host = "localhost";
        this.port = "3306";
        this.user = "root";
        this.pass = "root";
        this.database = "java";
        this.url = "jdbc:mysql://localhost:3306/java?useSSL=false";
        this.minInitialSize=5;
        this.maxActive=30;

        basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName(DRIVER);
        basicDataSource.setUsername(user);
        basicDataSource.setPassword(pass);
        basicDataSource.setUrl("jdbc:mysql://" + host + ":" + port + "/" + database);
        basicDataSource.setInitialSize(minInitialSize);
        basicDataSource.setMaxActive(maxActive);

        // Opcional. Sentencia SQL que le puede servir a BasicDataSource
        // para comprobar que la conexion es correcta.
        basicDataSource.setValidationQuery("select 1");

        this.dataSource = basicDataSource;
    }

    public DBPoolManager(String host, String port, String user, String pass, String database, String url, int minInitialSize, int maxActive) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.pass = pass;
        this.database = database;
        this.url = url;

        basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName(DRIVER);
        basicDataSource.setUsername(user);
        basicDataSource.setPassword(pass);
        basicDataSource.setUrl("jdbc:mysql://" + host + ":" + port + "/" + database);
        basicDataSource.setInitialSize(minInitialSize);
        basicDataSource.setMaxActive(maxActive);

        // Opcional. Sentencia SQL que le puede servir a BasicDataSource
        // para comprobar que la conexion es correcta.
        basicDataSource.setValidationQuery("select 1");

        this.dataSource = basicDataSource;
        
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
        this.url="jdbc:mysql://" + host + ":" + port + "/" + database;
        basicDataSource.setUrl(url);
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
        this.url="jdbc:mysql://" + host + ":" + port + "/" + database;
        basicDataSource.setUrl(url);
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
        this.url="jdbc:mysql://" + host + ":" + port + "/" + database;
        basicDataSource.setUrl(url);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getMinInitialSize() {
        return minInitialSize;
    }

    public int getMaxActive() {
        return maxActive;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private Connection dbConnect() throws Exception {
        Connection connection;
        try {

            connection = (Connection) dataSource.getConnection();
        } catch (SQLException ex) {
            throw new Exception("Se ha producido un error al conectar con la DB", ex);
        }
        return connection;

    }

    public void executeUpdate(String sql) {

        try {
            Connection connection = dbConnect();

            Statement st = connection.createStatement();
            st.executeUpdate(sql);

            st.close();
            dbCloseConnection(connection);
        } catch (Exception ex) {
            Logger.getLogger(DBPoolManager.class.getCanonicalName()).log(Level.SEVERE, ex.getLocalizedMessage(), ex);
        }
    }

    public void dbCloseConnection(Connection connection) throws Exception {
        try {
            connection.close();
        } catch (SQLException ex) {
            throw new Exception("Se ha producido un error al desconectar con la DB", ex);
        }

    }

    public ResultSet executeSelect(String sql) {

        ResultSet busqueda = null;
        CachedRowSet csr = null;
        try {

            Connection connection = dbConnect();
           
            try (Statement st = connection.createStatement()) {
                busqueda = st.executeQuery(sql);
                csr = new CachedRowSetImpl();
                csr.populate(busqueda);
            }

            dbCloseConnection(connection);
           

        } catch (Exception ex) {
            Logger.getLogger(DBPoolManager.class.getName()).log(Level.SEVERE, null, ex);
        }

        return csr;
    }

}
