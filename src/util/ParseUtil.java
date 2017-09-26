package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bean.ResultLine;

/**
 * ��־����������
 * 
 * @author: hshe-161202
 * @create date: 2017��7��14��
 * 
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ParseUtil {

	public ParseUtil() {

	}

	private String etlPath;
	private String etlName;
	private String fileModifyTime;
	private String jsonString;
	private Map jsonMap;
	private String queryText;
	private String hash;
	private String user;
	private Long timestamp;
	private List<Map> edges;
	private List<Map> vertices;
	private Map<Long, String> column = new HashMap<Long, String>();
	private List<ResultLine> resultList = new ArrayList<ResultLine>();

	public void parse(String json) throws ParseException {

		this.jsonString = json;

		this.resultList.clear();

		/**
		 * ���������json�ַ�����ת����map
		 */
		jsonMap = (Map) new JSONParser().parse(jsonString);

		/**
		 * ����queryText
		 */
		queryText = (String) jsonMap.get("queryText");

		/**
		 * ����hash
		 */
		hash = (String) jsonMap.get("hash");

		/**
		 * ����user
		 */
		user = (String) jsonMap.get("user");

		/**
		 * ����timestamp
		 */
		timestamp = (Long) jsonMap.get("timestamp");

		/**
		 * ����edges
		 */
		edges = (List<Map>) jsonMap.get("edges");

		/**
		 * ����vertices
		 */
		vertices = (List<Map>) jsonMap.get("vertices");

		/**
		 * �����ֶ��б�
		 */
		for (Map vertice : vertices) {
			column.put((Long) vertice.get("id"), vertice.get("vertexId").toString());
		}

		/**
		 * ����etlPath
		 */
		etlPath = parseEtlPath(queryText);

		/**
		 * ����etlName
		 */
		etlName = parseEtlName(queryText);

		/**
		 * ����fileModifyTime
		 */
		fileModifyTime = parseFileModifyTime(queryText);

		/**
		 * ����source��target��Ӧ��ϵ
		 */
		for (Map edge : edges) {

			if (edge.get("edgeType").equals("PROJECTION")) {

				for (Object source : (List) edge.get("sources")) {

					for (Object target : (List) edge.get("targets")) {

						ResultLine result = new ResultLine();

						String[] sourceRes = column.get(source).split("\\.");
						for (int i = 0; i < Math.min(3, sourceRes.length); i++) {
							result.setResult(i + 1, sourceRes[i].toLowerCase());
						}

						String[] targetRes = column.get(target).split("\\.");
						for (int i = 0; i < Math.min(3, targetRes.length); i++) {
							result.setResult(i + 4, targetRes[i].toLowerCase());
						}

						result.setEtlPath(etlPath);
						result.setEtlName(etlName);
						result.setFileModifyTime(fileModifyTime);

						resultList.add(result);

					}
				}
			}
		}
	}

	/**
	 * ��queryText��ƥ��ű�·��
	 * 
	 * @param queryText
	 * @return
	 */
	public String parseEtlPath(String queryText) {

		Pattern pattern = Pattern.compile("@@@1(.*)1@@@");
		Matcher matcher = pattern.matcher(queryText);

		if (matcher.find()) {
			return matcher.group().replaceAll("@@@1", "").replaceAll("1@@@", "");
		} else {
			return "0000";
		}
	}

	/**
	 * ��queryText��ƥ��ű�����
	 * 
	 * @param queryText
	 * @return
	 */
	public String parseEtlName(String queryText) {

		Pattern pattern = Pattern.compile("@@@2(.*)2@@@");
		Matcher matcher = pattern.matcher(queryText);

		if (matcher.find()) {
			return matcher.group().replaceAll("@@@2", "").replaceAll("2@@@", "");
		} else {
			return "0000";
		}
	}
	
	/**
	 * ��queryText��ƥ��ű�����޸�ʱ��
	 * 
	 * @param queryText
	 * @return
	 */
	public String parseFileModifyTime(String queryText) {

		Pattern pattern = Pattern.compile("@@@3(.*)3@@@");
		Matcher matcher = pattern.matcher(queryText);

		if (matcher.find()) {
			return matcher.group().replaceAll("@@@3", "").replaceAll("3@@@", "");
		} else {
			return "0000";
		}
	}

	public String getEtlPath() {
		return etlPath;
	}

	public void setEtlPath(String etlPath) {
		this.etlPath = etlPath;
	}

	public String getEtlName() {
		return etlName;
	}

	public void setEtlName(String etlName) {
		this.etlName = etlName;
	}

	public String getFileModifyTime() {
		return fileModifyTime;
	}

	public void setFileModifyTime(String fileModifyTime) {
		this.fileModifyTime = fileModifyTime;
	}

	public String getJsonString() {
		return jsonString;
	}

	public void setJsonString(String jsonString) {
		this.jsonString = jsonString;
	}

	public Map getJsonMap() {
		return jsonMap;
	}

	public void setJsonMap(Map jsonMap) {
		this.jsonMap = jsonMap;
	}

	public String getQueryText() {
		return queryText;
	}

	public void setQueryText(String queryText) {
		this.queryText = queryText;
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public List<Map> getEdges() {
		return edges;
	}

	public void setEdges(List<Map> edges) {
		this.edges = edges;
	}

	public List<Map> getVertices() {
		return vertices;
	}

	public void setVertices(List<Map> vertices) {
		this.vertices = vertices;
	}

	public Map<Long, String> getColumn() {
		return column;
	}

	public void setColumn(Map<Long, String> column) {
		this.column = column;
	}

	public List<ResultLine> getResultList() {
		return resultList;
	}

	public void setResultList(List<ResultLine> resultList) {
		this.resultList = resultList;
	}

	/**
	 * main
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		ParseUtil parseUtil = new ParseUtil();
		System.out.println(parseUtil.parseEtlPath("@@@111@@@"));
		System.out.println(parseUtil.parseEtlName("������222������"));

	}

}
