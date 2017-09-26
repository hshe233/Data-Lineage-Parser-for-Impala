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
 * �����������
 * 
 * @author: hshe-161202
 * @create date: 2017��8��14��
 * 
 */
public class DataSlicer {

	private Logger logger = LoggerFactory.getLogger(DataSlicer.class);

	public void doAction() {

		cleanData();
		deleteNode();

		logger.info("");
		logger.info("DataCleanActionִ�����!");

	}

	/**
	 * ��������ɾ���ظ�����
	 */
	public void cleanData() {
		/**
		 * ɾ���ظ�����
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

			logger.info("������ʱ������distinct���ݣ�\r\n    " + sqlTask1 + "\r\n    ");
			dbUtil.doUpdate(sqlTask1);

			logger.info("ɾ����������ݣ�\r\n    " + sqlTask2 + "\r\n    ");
			dbUtil.doUpdate(sqlTask2);

			logger.info("��distinct���ݲ�������_tmp��\r\n    " + sqlTask3 + "\r\n    ");
			dbUtil.doUpdate(sqlTask3);

			logger.info("ɾ����ʱ��_tmp��\r\n    " + sqlTask4 + "\r\n    ");
			dbUtil.doUpdate(sqlTask4);

		} catch (SQLException e) {
			logger.error("����ȥ�ش����쳣  " + e.getMessage());
			e.printStackTrace();
		} finally {
			dbUtil.close();
		}
	}

	/**
	 * ɾ����Ч��ڵ�
	 */
	public void deleteNode() {
		/**
		 * ���ӵ�lineage�����������ݿ⣬��ȡtarget���嵥
		 */
		String sql = "select  \r\n    " + "    t.targetschema, \r\n    " + "    t.targettable \r\n    " + "from \r\n    "
				+ "    lineage_info t \r\n    " + "group by \r\n    " + "    t.targetschema, \r\n    " + "    t.targettable";

		List<Map<String, Object>> distinctTblList = null;
		DBUtil dbUtil = new DBUtil(DB_TYPE.RESULT);

		try {
			distinctTblList = dbUtil.doSelect(sql);
		} catch (SQLException e) {
			logger.error("��ȡ������嵥�����쳣  <" + sql + "> :" + e.getMessage());
			e.printStackTrace();
		} finally {
			dbUtil.close();
		}

		/**
		 * ���ӵ�impalaԪ���ݿ⣬��һ�ж��嵥�еı��Ƿ���ڣ����ڲ����ڵı����ӵ�lineage�����������ݿ⣬ɾ����ؽڵ�
		 */
		DBUtil dbUtilMeta = new DBUtil(DB_TYPE.META);
		List<Map<String, Object>> tmpTblList = new ArrayList<Map<String, Object>>();

		for (Map<String, Object> tbl : distinctTblList) {

			String schema = (String) tbl.get("TARGETSCHEMA");
			String table = (String) tbl.get("TARGETTABLE");

			try {
				if (!dbUtilMeta.tableExists(schema, table)) {
					/**
					 * Ԫ���ݲ����ڵı�������Ч��ɾ����ؽڵ�
					 */
					tmpTblList.add(tbl);
				}
			} catch (SQLException e) {
				logger.error("�жϱ��Ƿ�����쳣 <" + schema + "." + table + "> .. :" + e.getMessage());
				e.printStackTrace();
			}
		}

		if (tmpTblList != null && tmpTblList.size() > 0) {

			DBUtil dbUtilRes = new DBUtil(DB_TYPE.RESULT);

			for (Map<String, Object> tbl : tmpTblList) {

				String schema = (String) tbl.get("TARGETSCHEMA");
				String table = (String) tbl.get("TARGETTABLE");

				/**
				 * Ԫ���ݲ����ڵı�������Ч��ɾ����ؽڵ�
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
