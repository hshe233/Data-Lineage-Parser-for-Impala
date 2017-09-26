package module;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.DBUtil;
import util.DBUtil.DB_TYPE;
import util.DeleteNodeUtil;
import util.PropertyFileUtil;

/**
 * 数据清理入口
 * 
 * @author: hshe-161202
 * @create date: 2017年8月14日
 * 
 */
public class DataSlicer {

	private Logger logger = LoggerFactory.getLogger(DataSlicer.class);

	public void doAction() {

		cleanData();
		deleteNode();

		logger.info("");
		logger.info("DataCleanAction执行完成!");

	}

	/**
	 * 数据清理，删除重复数据
	 */
	public void cleanData() {
		/**
		 * 删除重复数据
		 */
		DBUtil dbUtil = new DBUtil(DB_TYPE.RESULT);

		String sqlTask1 = "CREATE TABLE "
				+ PropertyFileUtil.getProperty("resultTableName")
				+ "_tmp AS \r\n    "
				+ "SELECT \r\n    "
				+ "  t.sourceschema,t.sourcetable,t.sourcecolumn,t.targetschema,t.targettable,t.targetcolumn,t.etlpath,t.etlname,t.filemodifytime\r\n    "
				+ "FROM\r\n    "
				+ "(\r\n    "
				+ "  select \r\n    "
				+ "    t.*,\r\n    "
				+ "    ROW_NUMBER()OVER(PARTITION BY t.sourceschema,t.sourcetable,t.sourcecolumn,t.targetschema,t.targettable,t.targetcolumn/*,t.etlpath,t.etlname*/ ORDER BY t.filemodifytime DESC) AS RAN\r\n    "
				+ "  from\r\n    " + "   " + PropertyFileUtil.getProperty("resultTableName") + " t\r\n    " + ") t\r\n    "
				+ "WHERE\r\n    " + "  RAN = 1 ";

		String sqlTask2 = "TRUNCATE TABLE " + PropertyFileUtil.getProperty("resultTableName");

		String sqlTask3 = "INSERT INTO " + PropertyFileUtil.getProperty("resultTableName") + "\r\n    " + "SELECT \r\n    "
				+ "  * \r\n    " + "FROM \r\n      " + PropertyFileUtil.getProperty("resultTableName") + "_tmp";

		String sqlTask4 = "DROP TABLE " + PropertyFileUtil.getProperty("resultTableName") + "_tmp";

		try {

			logger.info("创建临时表，保存distinct数据：\r\n    " + sqlTask1 + "\r\n    ");
			dbUtil.doUpdate(sqlTask1);

			logger.info("删除结果表数据：\r\n    " + sqlTask2 + "\r\n    ");
			dbUtil.doUpdate(sqlTask2);

			logger.info("将distinct数据插入结果表_tmp：\r\n    " + sqlTask3 + "\r\n    ");
			dbUtil.doUpdate(sqlTask3);

			logger.info("删除临时表_tmp：\r\n    " + sqlTask4 + "\r\n    ");
			dbUtil.doUpdate(sqlTask4);

		} catch (SQLException e) {
			logger.error("数据去重处理异常  " + e.getMessage());
			e.printStackTrace();
		} finally {
			dbUtil.close();
		}
	}

	/**
	 * 删除无效表节点
	 */
	public void deleteNode() {
		/**
		 * 连接到lineage结果保存的数据库，获取target表清单
		 */
		String sql = "select  \r\n    " + "    t.targetschema, \r\n    " + "    t.targettable \r\n    " + "from \r\n    "
				+ "    lineage_info t \r\n    " + "group by \r\n    " + "    t.targetschema, \r\n    " + "    t.targettable";

		List<Map<String, Object>> distinctTblList = null;
		DBUtil dbUtil = new DBUtil(DB_TYPE.RESULT);

		try {
			distinctTblList = dbUtil.doSelect(sql);
		} catch (SQLException e) {
			logger.error("获取结果表清单数据异常  <" + sql + "> :" + e.getMessage());
			e.printStackTrace();
		} finally {
			dbUtil.close();
		}

		/**
		 * 连接到impala元数据库，逐一判断清单中的表是否存在，对于不存在的表，连接到lineage结果保存的数据库，删除相关节点
		 */
		DBUtil dbUtilMeta = new DBUtil(DB_TYPE.META);
		List<Map<String, Object>> tmpTblList = new ArrayList<Map<String, Object>>();

		for (Map<String, Object> tbl : distinctTblList) {

			String schema = (String) tbl.get("TARGETSCHEMA");
			String table = (String) tbl.get("TARGETTABLE");

			try {
				if (!dbUtilMeta.tableExists(schema, table)) {
					/**
					 * 元数据不存在的表视作无效表，删除相关节点
					 */
					tmpTblList.add(tbl);
				}
			} catch (SQLException e) {
				logger.error("判断表是否存在异常 <" + schema + "." + table + "> .. :" + e.getMessage());
				e.printStackTrace();
			}
		}

		if (tmpTblList != null && tmpTblList.size() > 0) {

			DBUtil dbUtilRes = new DBUtil(DB_TYPE.RESULT);

			for (Map<String, Object> tbl : tmpTblList) {

				String schema = (String) tbl.get("TARGETSCHEMA");
				String table = (String) tbl.get("TARGETTABLE");

				/**
				 * 元数据不存在的表视作无效表，删除相关节点
				 */
				DeleteNodeUtil deleteNode = new DeleteNodeUtil(schema, table, "%", logger);
				deleteNode.doDelete(dbUtilRes);

			}

			dbUtilRes.close();
		}

		dbUtilMeta.close();
	}
	
	/**
	 * main
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		DataSlicer dataSlicer = new DataSlicer();
		dataSlicer.doAction();
	}
}
