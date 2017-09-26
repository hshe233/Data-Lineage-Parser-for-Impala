package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDriver;
import org.apache.commons.pool.impl.GenericObjectPool;

import util.DBUtil.DB_TYPE;

/**
 * 数据库连接池
 * 
 * @author: hshe-161202
 * @create date: 2017年8月14日
 * 
 */
public class ConnectionPool {

	private String driver;
	private String url;
	private String username;
	private String password;
	private String datasourcename;

	private static GenericObjectPool pool = null;

	public ConnectionPool(DB_TYPE type) {
		this.driver = PropertyFileUtil.getProperty(type.name().toLowerCase() + ".jdbc.driverClassName");
		this.url = PropertyFileUtil.getProperty(type.name().toLowerCase() + ".jdbc.url");
		this.username = PropertyFileUtil.getProperty(type.name().toLowerCase() + ".jdbc.username");
		this.password = PropertyFileUtil.getProperty(type.name().toLowerCase() + ".jdbc.password");
		this.datasourcename = "test";
	}

	public void init() {

		/**
		 * 如果连接池不为空，将其清空
		 */
		if (pool != null) {
			try {
				pool.close();
				pool = null;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/**
		 * 注册连接池
		 */
		int maxActive = 20;
		int minActive = 5;

		try {
			Class.forName(driver);
			GenericObjectPool connectionPool = new GenericObjectPool(null);
			connectionPool.setMaxActive(maxActive);
			connectionPool.setMaxIdle(maxActive);

			ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, username, password);
			PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory,
					connectionPool, null, null, false, true);

			Class.forName("org.apache.commons.dbcp.PoolingDriver");
			PoolingDriver poolingDriver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
			poolingDriver.registerPool(datasourcename, connectionPool);

			if (minActive > 0) {
				System.out.println("正在尝试连接数据库[" + datasourcename + "][" + url + "][" + username
						+ "/***],如果该数据源不通,则这里可能会阻塞很长时间....");
			}
			for (int j = 0; j < minActive; j++) {
				connectionPool.addObject();
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			PoolingDriver driver2 = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
			pool = (GenericObjectPool) driver2.getConnectionPool(datasourcename);
		} catch (SQLException e1) {
			e1.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public synchronized Connection getConnection() throws Exception {

		if (pool == null) {
			init();
		}

		Connection conn = null;

		if (pool != null) {
			conn = (java.sql.Connection) pool.borrowObject();
		}

		return conn;
	}

	public static void main(String[] args) {

		String sql = "select * from lineage_info where rownum < 10";

		ConnectionPool pool = new ConnectionPool(DB_TYPE.RESULT);

		Connection conn = null;
		try {
			conn = pool.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println(rs.getRow());
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
