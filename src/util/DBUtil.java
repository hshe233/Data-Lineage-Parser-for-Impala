package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bean.NodeLine;
import bean.ResultLine;
import bean.SourceNode;
import bean.TargetNode;

/**
 * 数据库工具类
 * 
 * @author: hshe-161202
 * @create date: 2017年7月4日
 * 
 */
public class DBUtil {

	private String driver;
	private String url;
	private String user;
	private String password;
	private String resultTableName;

	private Connection conn;

	public enum DB_TYPE {
		RESULT, META
	}

	public DBUtil(DB_TYPE type) {
		this.driver = PropertyFileUtil.getProperty(type.name().toLowerCase() + ".jdbc.driverClassName");
		this.url = PropertyFileUtil.getProperty(type.name().toLowerCase() + ".jdbc.url");
		this.user = PropertyFileUtil.getProperty(type.name().toLowerCase() + ".jdbc.username");
		this.password = PropertyFileUtil.getProperty(type.name().toLowerCase() + ".jdbc.password");
		this.resultTableName = PropertyFileUtil.getProperty("resultTableName");
	}

	public void setConn() throws SQLException {
		if (this.conn == null || this.conn.isClosed()) {
			try {
				Class.forName(driver);
				this.conn = DriverManager.getConnection(url, user, password);
			} catch (ClassNotFoundException classnotfoundexception) {
				classnotfoundexception.printStackTrace();
				System.err.println("db: " + classnotfoundexception.getMessage());
			} catch (SQLException sqlexception) {
				System.err.println("db.getconn(): " + sqlexception.getMessage());
			}
		}
	}

	public void resetConn() throws SQLException {
		this.conn.close();
		this.conn = null;
		try {
			Class.forName(driver);
			this.conn = DriverManager.getConnection(url, user, password);
		} catch (ClassNotFoundException classnotfoundexception) {
			classnotfoundexception.printStackTrace();
			System.err.println("db: " + classnotfoundexception.getMessage());
		} catch (SQLException sqlexception) {
			System.err.println("db.getconn(): " + sqlexception.getMessage());
		}
	}

	public int doInsert(String sql) throws SQLException {
		return doUpdate(sql);
	}

	public int doDelete(String sql) throws SQLException {
		return doUpdate(sql);
	}

	public int doUpdate(String sql) throws SQLException {
		Statement stmt = null;
		try {
			setConn();
			stmt = conn.createStatement();
			return stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public void doDelete(NodeLine node) throws SQLException {
		String sql1 = "delete from " + resultTableName + " where SOURCESCHEMA like '" + node.getSchema()
				+ "' and SOURCETABLE like '" + node.getTable() + "' and SOURCECOLUMN like '" + node.getColumn() + "'";
		String sql2 = "delete from " + resultTableName + " where TARGETSCHEMA like '" + node.getSchema()
				+ "' and TARGETTABLE like '" + node.getTable() + "' and TARGETCOLUMN like '" + node.getColumn() + "'";
		Statement stmt = null;
		try {
			setConn();
			stmt = conn.createStatement();
			stmt.executeUpdate(sql1);
			stmt.executeUpdate(sql2);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	public List<Map<String, Object>> doSelect(String sql) throws SQLException {
		Statement stmt = null;
		ResultSet rs = null;
		try {
			setConn();
			stmt = conn
					.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(sql);
			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			while (rs.next()) {
				Map<String, Object> map = rowToMap(rs, rs.getRow());
				list.add(map);
			}
			return list;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
	}

	private Map<String, Object> rowToMap(ResultSet resultset, int rowNum) throws SQLException {
		Map<String, Object> map = new HashMap<String, Object>();
		ResultSetMetaData rsmd = resultset.getMetaData();
		int columnNum = rsmd.getColumnCount();
		for (int i = 1; i <= columnNum; i++) {
			String columnName = rsmd.getColumnLabel(i);
			map.put(columnName, resultset.getObject(columnName));
		}
		return map;
	}

	public List<ResultLine> doSelect(SourceNode source) throws SQLException {

		String sql = "select * from " + resultTableName + " where SOURCESCHEMA = '" + source.getSchema()
				+ "' and SOURCETABLE = '" + source.getTable() + "' and SOURCECOLUMN = '" + source.getColumn() + "'";

		Statement stmt = null;
		ResultSet rs = null;
		try {
			setConn();
			stmt = conn
					.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(sql);
			List<ResultLine> list = new ArrayList<ResultLine>();
			while (rs.next()) {
				ResultLine rowToResultLine = rowToRes(rs);
				list.add(rowToResultLine);
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			rs.close();
			stmt.close();
		}
	}

	public List<ResultLine> doSelect(TargetNode target) throws SQLException {

		String sql = "select * from " + resultTableName + " where TARGETSCHEMA = '" + target.getSchema()
				+ "' and TARGETTABLE = '" + target.getTable() + "' and TARGETCOLUMN = '" + target.getColumn() + "'";

		Statement stmt = null;
		ResultSet rs = null;
		try {
			setConn();
			stmt = conn
					.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(sql);
			List<ResultLine> list = new ArrayList<ResultLine>();
			while (rs.next()) {
				ResultLine rowToResultLine = rowToRes(rs);
				list.add(rowToResultLine);
			}
			return list;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			rs.close();
			stmt.close();
		}
	}

	public List<ResultLine> doSelectLike(SourceNode source) throws SQLException {

		String sql = "select * from " + resultTableName + " where SOURCESCHEMA like '" + source.getSchema()
				+ "' and SOURCETABLE like '" + source.getTable() + "' and SOURCECOLUMN like '" + source.getColumn()
				+ "'";

		Statement stmt = null;
		ResultSet rs = null;
		try {
			setConn();
			stmt = conn
					.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(sql);
			List<ResultLine> list = new ArrayList<ResultLine>();
			while (rs.next()) {
				ResultLine rowToResultLine = rowToRes(rs);
				list.add(rowToResultLine);
			}
			return list;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			rs.close();
			stmt.close();
		}
	}

	public List<ResultLine> doSelectLike(TargetNode target) throws SQLException {

		String sql = "select * from " + resultTableName + " where TARGETSCHEMA like '" + target.getSchema()
				+ "' and TARGETTABLE like '" + target.getTable() + "' and TARGETCOLUMN like '" + target.getColumn()
				+ "'";

		Statement stmt = null;
		ResultSet rs = null;
		try {
			setConn();
			stmt = conn
					.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(sql);
			List<ResultLine> list = new ArrayList<ResultLine>();
			while (rs.next()) {
				ResultLine rowToResultLine = rowToRes(rs);
				list.add(rowToResultLine);
			}
			return list;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			rs.close();
			stmt.close();
		}
	}

	public List<ResultLine> doSelectRecursive(SourceNode source) throws SQLException {

		String sql = "SELECT \r\n    " + "    *\r\n    " + "FROM \r\n    " + "    " + resultTableName + "\r\n    "
				+ "WHERE \r\n    " + "        sourceschema <> targetschema \r\n    "
				+ "    or  sourcetable  <> targettable \r\n    " + "    or  sourcecolumn <> targetcolumn \r\n    "
				+ "CONNECT BY NOCYCLE\r\n    " + "        sourceschema = PRIOR targetschema \r\n    "
				+ "    and sourcetable  = PRIOR targettable \r\n    "
				+ "    and sourcecolumn = PRIOR targetcolumn \r\n    " + "START WITH \r\n    "
				+ "        sourceschema = '" + source.getSchema() + "' \r\n    " + "    and sourcetable  = '"
				+ source.getTable() + "' \r\n    " + "    and sourcecolumn = '" + source.getColumn() + "'";

		Statement stmt = null;
		ResultSet rs = null;
		try {
			setConn();
			stmt = conn
					.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(sql);
			List<ResultLine> list = new ArrayList<ResultLine>();
			while (rs.next()) {
				ResultLine rowToResultLine = rowToRes(rs);
				list.add(rowToResultLine);
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			rs.close();
			stmt.close();
		}
	}

	public List<ResultLine> doSelectRecursive(TargetNode target) throws SQLException {

		String sql = "SELECT \r\n    " + "    *\r\n    " + "FROM \r\n    " + "    " + resultTableName + "\r\n    "
				+ "WHERE \r\n    " + "        sourceschema <> targetschema \r\n    "
				+ "    or  sourcetable  <> targettable \r\n    " + "    or  sourcecolumn <> targetcolumn \r\n    "
				+ "CONNECT BY NOCYCLE\r\n    " + "        targetschema = PRIOR sourceschema \r\n    "
				+ "    and targettable  = PRIOR sourcetable\r\n    "
				+ "    and targetcolumn = PRIOR sourcecolumn \r\n    " + "START WITH \r\n    "
				+ "        targetschema = '" + target.getSchema() + "' \r\n    " + "    and targettable  = '"
				+ target.getTable() + "' \r\n    " + "    and targetcolumn = '" + target.getColumn() + "'";

		Statement stmt = null;
		ResultSet rs = null;
		try {
			setConn();
			stmt = conn
					.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(sql);
			List<ResultLine> list = new ArrayList<ResultLine>();
			while (rs.next()) {
				ResultLine rowToResultLine = rowToRes(rs);
				list.add(rowToResultLine);
			}
			return list;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			rs.close();
			stmt.close();
		}
	}

	private ResultLine rowToRes(ResultSet resultset) throws SQLException {

		ResultLine resultLine = new ResultLine();

		SourceNode sourceNode = new SourceNode((String) resultset.getObject("SOURCESCHEMA"),
				(String) resultset.getObject("SOURCETABLE"), (String) resultset.getObject("SOURCECOLUMN"));
		TargetNode targetNode = new TargetNode((String) resultset.getObject("TARGETSCHEMA"),
				(String) resultset.getObject("TARGETTABLE"), (String) resultset.getObject("TARGETCOLUMN"));

		resultLine.setSourceNode(sourceNode);
		resultLine.setTargetNode(targetNode);

		resultLine.setEtlName((String) resultset.getObject("ETLNAME"));
		resultLine.setEtlPath((String) resultset.getObject("ETLPATH"));
		resultLine.setFileModifyTime((String) resultset.getObject("FILEMODIFYTIME"));

		return resultLine;
	}

	public void doInsertBatch(List<ResultLine> parseResultListAll) throws SQLException {

		PreparedStatement pst = null;

		try {
			setConn();
			conn.setAutoCommit(false);
			pst = conn.prepareStatement("INSERT INTO " + resultTableName + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");

			for (ResultLine result : parseResultListAll) {

				/**
				 * 由于Oracle对于null值不便处理，对于空值，转换为字符串"null"插入
				 */
				pst.setString(1, result.getSourceNode().getSchema() == null ? "null" : result.getSourceNode()
						.getSchema());
				pst.setString(2, result.getSourceNode().getTable() == null ? "null" : result.getSourceNode().getTable());
				pst.setString(3, result.getSourceNode().getColumn() == null ? "null" : result.getSourceNode()
						.getColumn());
				pst.setString(4, result.getTargetNode().getSchema() == null ? "null" : result.getTargetNode()
						.getSchema());
				pst.setString(5, result.getTargetNode().getTable() == null ? "null" : result.getTargetNode().getTable());
				pst.setString(6, result.getTargetNode().getColumn() == null ? "null" : result.getTargetNode()
						.getColumn());
				pst.setString(7, result.getEtlPath() == null ? "null" : result.getEtlPath());
				pst.setString(8, result.getEtlName() == null ? "null" : result.getEtlName());
				pst.setString(9, result.getFileModifyTime() == null ? "null" : result.getFileModifyTime());

				pst.addBatch();

			}

			pst.executeBatch();
			conn.commit();

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			pst.clearBatch();
			pst.close();
		}

	}

	public void doInsertBatch2(List<ResultLine> parseResultListAll) throws SQLException {

		PreparedStatement pst = null;

		try {
			setConn();
			conn.setAutoCommit(false);
			pst = conn.prepareStatement("INSERT INTO " + resultTableName + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");

			for (ResultLine result : parseResultListAll) {

				/**
				 * 由于Oracle对于null值不便处理，对于空值，转换为字符串"null"插入
				 */
				pst.setString(1, result.getSourceNode().getSchema() == null ? "null" : result.getSourceNode()
						.getSchema());
				pst.setString(2, result.getSourceNode().getTable() == null ? "null" : result.getSourceNode().getTable());
				pst.setString(3, result.getSourceNode().getColumn() == null ? "null" : result.getSourceNode()
						.getColumn());
				pst.setString(4, result.getTargetNode().getSchema() == null ? "null" : result.getTargetNode()
						.getSchema());
				pst.setString(5, result.getTargetNode().getTable() == null ? "null" : result.getTargetNode().getTable());
				pst.setString(6, result.getTargetNode().getColumn() == null ? "null" : result.getTargetNode()
						.getColumn());
				pst.setString(7, result.getEtlPath() == null ? "null" : result.getEtlPath());
				pst.setString(8, result.getEtlName() == null ? "null" : result.getEtlName());
				pst.setString(9, result.getFileModifyTime() == null ? "null" : result.getFileModifyTime());

				pst.addBatch();

			}

			pst.executeBatch();
			conn.commit();

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			pst.clearBatch();
			pst.close();
		}

	}

	public boolean tableExists(String schema, String table) throws SQLException {

		String sql = "SELECT\r\n    " + "    tab.\"TBL_ID\",\r\n    " + "    tab.\"CREATE_TIME\",\r\n    "
				+ "    tab.\"DB_ID\",\r\n    " + "    tab.\"LAST_ACCESS_TIME\",\r\n    " + "    tab.\"OWNER\",\r\n    "
				+ "    tab.\"RETENTION\",\r\n    " + "    tab.\"SD_ID\",\r\n    " + "    tab.\"TBL_NAME\",\r\n    "
				+ "    tab.\"TBL_TYPE\",\r\n    " + "    tab.\"VIEW_EXPANDED_TEXT\",\r\n    "
				+ "    tab.\"VIEW_ORIGINAL_TEXT\" \r\n    " + "FROM \r\n    " + "    \"TBLS\" tab,\r\n    "
				+ "    \"DBS\" db\r\n    " + "WHERE\r\n    " + "    tab.\"DB_ID\" = db.\"DB_ID\"\r\n    "
				+ "    and db.\"NAME\" = '" + schema + "'" + "    and tab.\"TBL_NAME\" = '" + table + "'";

		List<Map<String, Object>> resList = this.doSelect(sql);

		return !resList.isEmpty();
	}

	public void close() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				System.out.println("关闭数据库连接时发生异常 .. 正在强行关闭..");
				e.printStackTrace();
			} finally {
				conn = null;
			}
		}
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

	public String getResultTableName() {
		return resultTableName;
	}

	public void setResultTableName(String resultTableName) {
		this.resultTableName = resultTableName;
	}

	/**
	 * main
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		long startTime = System.currentTimeMillis();

		DBUtil dbUtil = new DBUtil(DB_TYPE.RESULT);
		TargetNode sl = new TargetNode("mdata", "m21_adm_rate_detail_cur1", "tax_rate");

		TargetNode node1 = new TargetNode("pwork", "lineage_test_sh", "km02avaflg");
		SourceNode node2 = new SourceNode("pdata", "t03_deposit_acct_base_info", "acct_id");
		TargetNode node3 = new TargetNode("pdata", "t00_exch_rate_info", "cnvt_exch_rate");
		SourceNode node4 = new SourceNode("pdata", "t00_exch_rate_info", "cnvt_exch_rate");

		List<ResultLine> rsList = dbUtil.doSelect(node4);

		for (ResultLine rs : rsList) {
			System.out.println(rs.toString());
		}

		System.out.println("结果集条数： " + rsList.size());

		long endTime = System.currentTimeMillis();
		System.out.println("查询完成!耗时 " + Float.toString((endTime - startTime) / 1000F) + " 秒");
	}
}
