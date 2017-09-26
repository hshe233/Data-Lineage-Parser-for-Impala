package bean;

/**
 * 结果表数据结构
 * 
 * @author: hshe-161202
 * @create date: 2017年7月28日
 * 
 */

public class ResultLine {

	private SourceNode sourceNode = new SourceNode();

	private TargetNode targetNode = new TargetNode();

	private String etlPath;

	private String etlName;
	
	private String fileModifyTime;

	public ResultLine() {
		
	}

	public ResultLine(SourceNode src, TargetNode tar) {

		this.sourceNode = src;
		this.targetNode = tar;

	}

	public void setResult(int i, String str) {
		switch (i) {
		case 1:
			this.sourceNode.setSchema(str);
			break;
		case 2:
			this.sourceNode.setTable(str); 
			break;
		case 3:
			this.sourceNode.setColumn(str);
			break;
		case 4:
			this.targetNode.setSchema(str);
			break;
		case 5:
			this.targetNode.setTable(str);
			break;
		case 6:
			this.targetNode.setColumn(str);
			break;
		}
	}

	public String toString() {
		return "SOURCE: " + this.sourceNode.toString() + "\r\nTARGET: " + this.targetNode.toString();
	}

	public SourceNode getSourceNode() {
		return sourceNode;
	}

	public void setSourceNode(SourceNode sourceNode) {
		this.sourceNode = sourceNode;
	}

	public TargetNode getTargetNode() {
		return targetNode;
	}

	public void setTargetNode(TargetNode targetNode) {
		this.targetNode = targetNode;
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
}
