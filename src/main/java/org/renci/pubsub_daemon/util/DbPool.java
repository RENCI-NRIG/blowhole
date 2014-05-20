package org.renci.pubsub_daemon.util;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import org.renci.pubsub_daemon.Globals;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;

/**
 * Create and maintain a pool of connections to the database
 * with specific parameters
 * @author ibaldin
 *
 */
public class DbPool {
	protected ComboPooledDataSource cpds = null;
	protected String url;
	
	public DbPool(String url, String user, String pass) {
		this.url = url;
		
		Globals.info("Initializing connection pool for " + url);
		
		// if all non-null, create a pooled connection source to the db
		if ((url == null) || (user == null) || (pass == null)) {
			Globals.error("Insufficient database parameters, not creating a connection pool");
			cpds = null;
			return;
		}
		
		cpds = new ComboPooledDataSource();
		try {
			cpds.setDriverClass("com.mysql.jdbc.Driver");
			cpds.setJdbcUrl(url);
			cpds.setUser(user);
			cpds.setPassword(pass);
		} catch (PropertyVetoException e) {
			Globals.error("Unable to create JDBC MySQL connection pool (non-fatal): " + e);
			try {
				DataSources.destroy(cpds);
			} catch (SQLException e1) {
			}
			cpds = null;
		}
	}
	
	/**
	 * Is this pool valid
	 * @return
	 */
	public boolean poolValid() {
		return (cpds != null);
	}
	
	public String getUrl() {
		return new String(url);
	}
	
	/**
	 * Get a new connection from the pool
	 * @return
	 * @throws SQLException
	 */
	public synchronized Connection getDbConnection() throws SQLException {
		if (cpds != null) {
			return cpds.getConnection();
		}
		throw new SQLException("Invalid database parameters");
	}
}
