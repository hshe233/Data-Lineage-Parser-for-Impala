package ztest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDriver;
import org.apache.commons.pool.impl.GenericObjectPool;


public class dbcpTest {

	public static void main(String[] args) {

		String str_user = "edap";
		String str_pwd = "edap";
		String str_dbdriver = "oracle.jdbc.driver.OracleDriver";
		int li_maxsize = 20;
		int li_initsize = 5;
		String str_dburl = "jdbc:Oracle:thin:@12.99.106.121:1521:orcl";
		String str_datasourcename = "test";

		try {
			Class.forName(str_dbdriver);
			GenericObjectPool connectionPool = new GenericObjectPool(null); // 创建一个连接池!!
			connectionPool.setMaxActive(li_maxsize); // 设置最大活动数
			connectionPool.setMaxIdle(li_maxsize); //

			ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(str_dburl, str_user, str_pwd);
			PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory,
					connectionPool, null, null, false, true); //
			Class.forName("org.apache.commons.dbcp.PoolingDriver"); // 创建dbcp池驱动
			PoolingDriver driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
			driver.registerPool(str_datasourcename, connectionPool); // 注册连接池

			if (li_initsize > 0) {
				System.out.println("正在尝试连接数据库[" + str_datasourcename + "][" + str_dburl + "][" + str_user
						+ "/***],如果该数据源不通,则这里可能会阻塞很长时间....");
			}
			for (int j = 0; j < li_initsize; j++) {
				connectionPool.addObject(); // 要错就错在这里!!!如果数据库连不上,这里会报错!
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // 创建数据源
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		PoolingDriver driver;
		try {
			driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
			GenericObjectPool pool = (GenericObjectPool) driver.getConnectionPool(str_datasourcename);
			Connection conn = (java.sql.Connection) pool.borrowObject();
			conn.close();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

		
	}
}

