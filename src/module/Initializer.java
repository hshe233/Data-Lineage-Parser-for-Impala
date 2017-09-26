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
 * 批量处理入口
 * 
 * @author: hshe-161202
 * @create date: 2017年8月9日
 * 
 */
public class Initializer {

	private Logger logger = LoggerFactory.getLogger(Initializer.class);
	
	public void doAction() {

		long startTime = System.currentTimeMillis();

		/**
		 * 下载文本
		 */
		List<String> fileList = downloadLogs();

		//List<String> fileList = getFileList();

		long endTime1 = System.currentTimeMillis();
		logger.info("文本下载耗时 " + Float.toString((endTime1 - startTime) / 1000F) + " 秒");

		for (String file : fileList) {

			file = PropertyFileUtil.getProperty("localFileDirectory") + file;

			/**
			 * 解析单个文本
			 */
			List<ResultLine> parseResultListAll = parseLog(file);

			/**
			 * 保存到数据库
			 */

			DBUtil dbUtil = new DBUtil(DB_TYPE.RESULT);
			saveResult(parseResultListAll, dbUtil);
			dbUtil.close();

		}
		
		/**
		 * 删除重复数据和无效表节点
		 */
		DataSlicer dataCleanAction = new DataSlicer();
		dataCleanAction.doAction();
		
		long endTime = System.currentTimeMillis();
		logger.info("执行完成!耗时 " + Float.toString((endTime - startTime) / 1000F) + " 秒");

	}

	/**
	 * 文件下载
	 * 
	 * @return
	 */
	private List<String> downloadLogs() {

		logger.info("开始下载文本。。");

		/**
		 * 下载所有节点上的日志文件
		 */
		for (int i = 1; i < Integer.valueOf(PropertyFileUtil.getProperty("node_num")) + 1; i++) {

			FileUtil fileUtil = new FileUtil();
			ChannelSftp channelSftp = null;
			
			logger.info("正在连接到 " + (PropertyFileUtil.getProperty("host" + i)) + " ..");

			try {
				channelSftp = fileUtil.channelSftp(PropertyFileUtil.getProperty("host" + i),
						Integer.valueOf(PropertyFileUtil.getProperty("port" + i)),
						PropertyFileUtil.getProperty("user" + i), PropertyFileUtil.getProperty("password" + i));

				fileUtil.downloadByDirectory(PropertyFileUtil.getProperty("remoteFileDirectory"),
						PropertyFileUtil.getProperty("localFileDirectory"), channelSftp);

			} catch (JSchException e) {
				logger.error("日志下载失败  " + e.getMessage());
				e.printStackTrace();
			} finally {
				channelSftp.disconnect();
				channelSftp.exit();
			}
			
			logger.info("");
		}

		List<String> logList = getFileList();
		logger.info("文本下载完成！共下载 " + logList.size() + " 个日志文件");
		return logList;

	}

	/**
	 * 获取下载到本地的所有文件列表
	 */
	private List<String> getFileList() {

		List<String> logList = null;
		FileUtil fileUtil1 = new FileUtil();
		try {
			logList = fileUtil1.listFiles(PropertyFileUtil.getProperty("localFileDirectory"));
		} catch (Exception e) {
			logger.error("获取日志列表异常  " + e.getMessage());
			e.printStackTrace();
		}

		return logList;
	}

	/**
	 * 文件解析
	 * 
	 * @param log
	 * @return
	 */
	private List<ResultLine> parseLog(String log) {

		logger.info("开始解析文本  " + log);

		BufferedReader buffererReader = null;
		List<ResultLine> parseResultListAll = new ArrayList<ResultLine>();

		try {
			buffererReader = new BufferedReader(new InputStreamReader(new FileInputStream(log), "UTF-8"));

			String jsonString;
			while ((jsonString = buffererReader.readLine()) != null) {

				/**
				 * 解析json日志，转换为map
				 */
				ParseUtil parseUtil = new ParseUtil();
				parseUtil.parse(jsonString);

				/**
				 * 分析除了select开头的query
				 */
				String queryText = parseUtil.getQueryText();
				if (!queryText.toUpperCase().replaceAll("\\s*", "").startsWith("SELECT")) {

					List<ResultLine> parseResultList = parseUtil.getResultList();

					parseResultListAll.addAll(parseResultList);

				}
			}

		} catch (UnsupportedEncodingException e) {
			logger.error("文件读取异常：不支持的编码格式  " + e.getMessage());
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			logger.error("文件读取异常：文件不存在  " + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("文件读取异常：realLine异常  " + e.getMessage());
			e.printStackTrace();
		} catch (ParseException e) {
			logger.error("json解析异常  " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				buffererReader.close();
			} catch (IOException e) {
				logger.error("文件关闭异常  " + e.getMessage());
				e.printStackTrace();
			}
		}

		logger.info("文本解析完成，生成血缘信息 " + parseResultListAll.size() + " 条");

		return parseResultListAll;
	}

	/**
	 * 结果保存
	 * 
	 * @param resultList
	 */
	private void saveResult(List<ResultLine> parseResultListAll, DBUtil dbUtil) {

		logger.info("开始数据入库 。。");

		try {

			dbUtil.doInsertBatch(parseResultListAll);

		} catch (SQLException e) {

			logger.error("数据入库异常  " + e.getMessage() + " .. 重跑数据入库 ..");
			/**
			 * 报错重跑（如果不是随机性报错可能会死循环）
			 */
			saveResult(parseResultListAll, dbUtil);

		} 

		logger.info("数据入库完成！");
	}

	public static void main(String[] args) {

		Initializer initializer = new Initializer();
		initializer.doAction();

	}
}
