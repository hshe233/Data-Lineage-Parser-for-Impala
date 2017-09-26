package module;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.List;

import kafka.KafkaConsumer;
import kafka.consumer.ConsumerIterator;

import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bean.ResultLine;
import util.DBUtil;
import util.ParseUtil;
import util.DBUtil.DB_TYPE;
import util.PropertyFileUtil;

/**
 * 实时处理入口
 * 
 * @author: hshe-161202
 * @create date: 2017年8月14日
 * 
 */
public class Parser {

	private Logger logger = LoggerFactory.getLogger(Parser.class);

	public void doAction() {

		/**
		 * 定义Kafka主题
		 */
		String topic = PropertyFileUtil.getProperty("topic");

		KafkaConsumer kafkaConsumer = new KafkaConsumer();
		ConsumerIterator<byte[], byte[]> it = kafkaConsumer.iterator(topic);

		DBUtil dbUtil = new DBUtil(DB_TYPE.RESULT);

		int count = 0;

		/**
		 * 循环消费Kafka数据
		 */
		while (it.hasNext()) {

			String logString = null;

			try {
				logString = new String(it.next().message(), PropertyFileUtil.getProperty("encoding"));
			} catch (UnsupportedEncodingException e) {
				logger.error("错误的日志编码格式 : " + PropertyFileUtil.getProperty("encoding"));
				e.printStackTrace();
			}
			
			try {
				if (logString != null && logString.length() > 0) {
					parse(logString, dbUtil);
				}
			} catch (ParseException e) {
				logger.warn("json解析异常 [" + e.getMessage() + "]: " + logString + " .. 跳过该段落 ..");
				e.printStackTrace();
				continue;
			}

			count++;
			logger.info("Totally Parsed <" + count + "> SQL-QueryTexts Since Latest Start ..");
		}

		dbUtil.close();

	}
	
	public void parse(String logString, DBUtil dbUtil) throws ParseException {

		/**
		 * 解析jsonString
		 */
		List<ResultLine> parseResultList = parseLog(logString);

		/**
		 * 保存到数据库
		 */
		if (parseResultList != null) {
			saveResult(parseResultList, dbUtil);
		}
	}

	/**
	 * 文本解析
	 * 
	 * @param logString
	 * @return
	 * @throws ParseException 
	 */
	private List<ResultLine> parseLog(String logString) throws ParseException {

		/**
		 * 解析日志，转换为map
		 */
		ParseUtil parseUtil = new ParseUtil();
		parseUtil.parse(logString);

		/**
		 * 分析除了select开头的query
		 */
		List<ResultLine> parseResultList = null;

		if (!parseUtil.getQueryText().toUpperCase().replaceAll("\\s*", "").startsWith("SELECT")) {
			parseResultList = parseUtil.getResultList();
		}

		return parseResultList;
	}

	/**
	 * 结果保存
	 * 
	 * @param resultList
	 */
	private void saveResult(List<ResultLine> parseResultList, DBUtil dbUtil) {

		try {

			dbUtil.doInsertBatch2(parseResultList);

		} catch (SQLException e) {

			logger.error("SQL执行异常  " + e.getMessage() + " .. 开始重试 ..");

			/**
			 * 报错重跑（如果不是随机性报错可能会死循环）
			 */
			saveResult(parseResultList, dbUtil);

			e.printStackTrace();

		}
	}

	public static void main(String[] args) {
		Parser parseAction = new Parser();
		parseAction.doAction();
	}
}
