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
 * ʵʱ�������
 * 
 * @author: hshe-161202
 * @create date: 2017��8��14��
 * 
 */
public class Parser {

	private Logger logger = LoggerFactory.getLogger(Parser.class);

	public void doAction() {

		/**
		 * ����Kafka����
		 */
		String topic = PropertyFileUtil.getProperty("topic");

		KafkaConsumer kafkaConsumer = new KafkaConsumer();
		ConsumerIterator<byte[], byte[]> it = kafkaConsumer.iterator(topic);

		DBUtil dbUtil = new DBUtil(DB_TYPE.RESULT);

		int count = 0;

		/**
		 * ѭ������Kafka����
		 */
		while (it.hasNext()) {

			String logString = null;

			try {
				logString = new String(it.next().message(), PropertyFileUtil.getProperty("encoding"));
			} catch (UnsupportedEncodingException e) {
				logger.error("�������־�����ʽ : " + PropertyFileUtil.getProperty("encoding"));
				e.printStackTrace();
			}
			
			try {
				if (logString != null && logString.length() > 0) {
					parse(logString, dbUtil);
				}
			} catch (ParseException e) {
				logger.warn("json�����쳣 [" + e.getMessage() + "]: " + logString + " .. �����ö��� ..");
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
		 * ����jsonString
		 */
		List<ResultLine> parseResultList = parseLog(logString);

		/**
		 * ���浽���ݿ�
		 */
		if (parseResultList != null) {
			saveResult(parseResultList, dbUtil);
		}
	}

	/**
	 * �ı�����
	 * 
	 * @param logString
	 * @return
	 * @throws ParseException 
	 */
	private List<ResultLine> parseLog(String logString) throws ParseException {

		/**
		 * ������־��ת��Ϊmap
		 */
		ParseUtil parseUtil = new ParseUtil();
		parseUtil.parse(logString);

		/**
		 * ��������select��ͷ��query
		 */
		List<ResultLine> parseResultList = null;

		if (!parseUtil.getQueryText().toUpperCase().replaceAll("\\s*", "").startsWith("SELECT")) {
			parseResultList = parseUtil.getResultList();
		}

		return parseResultList;
	}

	/**
	 * �������
	 * 
	 * @param resultList
	 */
	private void saveResult(List<ResultLine> parseResultList, DBUtil dbUtil) {

		try {

			dbUtil.doInsertBatch2(parseResultList);

		} catch (SQLException e) {

			logger.error("SQLִ���쳣  " + e.getMessage() + " .. ��ʼ���� ..");

			/**
			 * �������ܣ������������Ա�����ܻ���ѭ����
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
