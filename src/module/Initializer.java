package module;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.DBUtil;
import util.DBUtil.DB_TYPE;
import util.FileUtil;
import util.ParseUtil;
import util.PropertyFileUtil;
import bean.ResultLine;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;

/**
 * �����������
 * 
 * @author: hshe-161202
 * @create date: 2017��8��9��
 * 
 */
public class Initializer {

	private Logger logger = LoggerFactory.getLogger(Initializer.class);
	
	public void doAction() {

		long startTime = System.currentTimeMillis();

		/**
		 * �����ı�
		 */
		List<String> fileList = downloadLogs();

		//List<String> fileList = getFileList();

		long endTime1 = System.currentTimeMillis();
		logger.info("�ı����غ�ʱ " + Float.toString((endTime1 - startTime) / 1000F) + " ��");

		for (String file : fileList) {

			file = PropertyFileUtil.getProperty("localFileDirectory") + file;

			/**
			 * ���������ı�
			 */
			List<ResultLine> parseResultListAll = parseLog(file);

			/**
			 * ���浽���ݿ�
			 */

			DBUtil dbUtil = new DBUtil(DB_TYPE.RESULT);
			saveResult(parseResultListAll, dbUtil);
			dbUtil.close();

		}
		
		/**
		 * ɾ���ظ����ݺ���Ч��ڵ�
		 */
		DataSlicer dataCleanAction = new DataSlicer();
		dataCleanAction.doAction();
		
		long endTime = System.currentTimeMillis();
		logger.info("ִ�����!��ʱ " + Float.toString((endTime - startTime) / 1000F) + " ��");

	}

	/**
	 * �ļ�����
	 * 
	 * @return
	 */
	private List<String> downloadLogs() {

		logger.info("��ʼ�����ı�����");

		/**
		 * �������нڵ��ϵ���־�ļ�
		 */
		for (int i = 1; i < Integer.valueOf(PropertyFileUtil.getProperty("node_num")) + 1; i++) {

			FileUtil fileUtil = new FileUtil();
			ChannelSftp channelSftp = null;
			
			logger.info("�������ӵ� " + (PropertyFileUtil.getProperty("host" + i)) + " ..");

			try {
				channelSftp = fileUtil.channelSftp(PropertyFileUtil.getProperty("host" + i),
						Integer.valueOf(PropertyFileUtil.getProperty("port" + i)),
						PropertyFileUtil.getProperty("user" + i), PropertyFileUtil.getProperty("password" + i));

				fileUtil.downloadByDirectory(PropertyFileUtil.getProperty("remoteFileDirectory"),
						PropertyFileUtil.getProperty("localFileDirectory"), channelSftp);

			} catch (JSchException e) {
				logger.error("��־����ʧ��  " + e.getMessage());
				e.printStackTrace();
			} finally {
				channelSftp.disconnect();
				channelSftp.exit();
			}
			
			logger.info("");
		}

		List<String> logList = getFileList();
		logger.info("�ı�������ɣ������� " + logList.size() + " ����־�ļ�");
		return logList;

	}

	/**
	 * ��ȡ���ص����ص������ļ��б�
	 */
	private List<String> getFileList() {

		List<String> logList = null;
		FileUtil fileUtil1 = new FileUtil();
		try {
			logList = fileUtil1.listFiles(PropertyFileUtil.getProperty("localFileDirectory"));
		} catch (Exception e) {
			logger.error("��ȡ��־�б��쳣  " + e.getMessage());
			e.printStackTrace();
		}

		return logList;
	}

	/**
	 * �ļ�����
	 * 
	 * @param log
	 * @return
	 */
	private List<ResultLine> parseLog(String log) {

		logger.info("��ʼ�����ı�  " + log);

		BufferedReader buffererReader = null;
		List<ResultLine> parseResultListAll = new ArrayList<ResultLine>();

		try {
			buffererReader = new BufferedReader(new InputStreamReader(new FileInputStream(log), "UTF-8"));

			String jsonString;
			while ((jsonString = buffererReader.readLine()) != null) {

				/**
				 * ����json��־��ת��Ϊmap
				 */
				ParseUtil parseUtil = new ParseUtil();
				parseUtil.parse(jsonString);

				/**
				 * ��������select��ͷ��query
				 */
				String queryText = parseUtil.getQueryText();
				if (!queryText.toUpperCase().replaceAll("\\s*", "").startsWith("SELECT")) {

					List<ResultLine> parseResultList = parseUtil.getResultList();

					parseResultListAll.addAll(parseResultList);

				}
			}

		} catch (UnsupportedEncodingException e) {
			logger.error("�ļ���ȡ�쳣����֧�ֵı����ʽ  " + e.getMessage());
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			logger.error("�ļ���ȡ�쳣���ļ�������  " + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("�ļ���ȡ�쳣��realLine�쳣  " + e.getMessage());
			e.printStackTrace();
		} catch (ParseException e) {
			logger.error("json�����쳣  " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				buffererReader.close();
			} catch (IOException e) {
				logger.error("�ļ��ر��쳣  " + e.getMessage());
				e.printStackTrace();
			}
		}

		logger.info("�ı�������ɣ�����ѪԵ��Ϣ " + parseResultListAll.size() + " ��");

		return parseResultListAll;
	}

	/**
	 * �������
	 * 
	 * @param resultList
	 */
	private void saveResult(List<ResultLine> parseResultListAll, DBUtil dbUtil) {

		logger.info("��ʼ������� ����");

		try {

			dbUtil.doInsertBatch(parseResultListAll);

		} catch (SQLException e) {

			logger.error("��������쳣  " + e.getMessage() + " .. ����������� ..");
			/**
			 * �������ܣ������������Ա�����ܻ���ѭ����
			 */
			saveResult(parseResultListAll, dbUtil);

		} 

		logger.info("���������ɣ�");
	}

	public static void main(String[] args) {

		Initializer initializer = new Initializer();
		initializer.doAction();

	}
}
