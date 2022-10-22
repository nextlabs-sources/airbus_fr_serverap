package com.nextlabs.db;

import java.sql.Connection;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * A high performance database connection datasource
 * 
 * @author klee
 *
 */
public class HikariCPDataSource {
    
    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;
          
    /**
     * Retrieve the database connection from data source
     * @return Database connection
     * @throws SQLException Database Exception
     */
    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
     
    public HikariCPDataSource(){}
    
    /**
     * Initialize the datasource with UserDBProfile information
     * @param profile UserDBProfile which contain database information
     */
    public HikariCPDataSource(UserDBProfile profile) {
    	config.setDriverClassName(profile.getDatabaseDriverName());
    	config.setJdbcUrl(profile.getConnectionUrl());
        config.setUsername(profile.getUserName());
        config.setSchema(profile.getSchema());
        config.setPassword(profile.getDecryptedPassword());
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        ds = new HikariDataSource(config);
    }
    
    /**
     * Initialize the datasource with ResourceDBProfile information
     * @param profile ResourceDBProfile which contain database information
     */
    public HikariCPDataSource(ResourceDBProfile profile) {
    	config.setDriverClassName(profile.getDatabaseDriverName());
    	config.setJdbcUrl(profile.getConnectionUrl());
        config.setUsername(profile.getUserName());
        config.setSchema(profile.getSchema());
        config.setPassword(profile.getDecryptedPassword());
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        ds = new HikariDataSource(config);
    }
}