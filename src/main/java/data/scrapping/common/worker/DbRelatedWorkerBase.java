package data.scrapping.common.worker;

import java.sql.Connection;
import java.sql.DriverManager;

public abstract class DbRelatedWorkerBase extends WorkerBase {

	private String dbConnString;
	private String dbUsername;
	private String dbPassword;
	private Connection conn;

	public String getDbConnString() {
		return dbConnString;
	}

	public void setDbConnString(String dbConnString) {
		this.dbConnString = dbConnString;
	}

	public String getDbUsername() {
		return dbUsername;
	}

	public void setDbUsername(String dbUsername) {
		this.dbUsername = dbUsername;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

	protected Connection getConn() throws Exception {
		if (conn == null) {
			conn = DriverManager.getConnection(dbConnString, dbUsername, dbPassword);
		}
		return conn;
	}

}
