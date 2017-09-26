package ztest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ImpalaJdbcTest {
	
	static String JDBC_DRIVER = "com.cloudera.impala.jdbc4.Driver";
	static String CONNECTION_URL = "jdbc:impala://8.99.5.202:21050/db_1";

	public static void main(String[] args) {
		Connection con = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		
		try {
			Class.forName(JDBC_DRIVER);
			con = DriverManager.getConnection(CONNECTION_URL);
			ps = con.prepareStatement("SELECT * FROM sdata.gjj_acc_inf_bigdata");
			rs = ps.executeQuery();
			
			while (rs.next()) {
				System.out.println(rs.getString(1));
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
