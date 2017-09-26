package bean;

/**
 * 源节点
 * 
 * @author: hshe-161202
 * @create date: 2017年7月24日
 * 
 */
public class SourceNode extends NodeLine {

	public SourceNode() {
		super();
	}
	
	public SourceNode(String name) {
		super(name);
	}

	public SourceNode(String schema, String table, String column) {
		super(schema, table, column);
	}

	public TargetNode toTargetLine() {
		return new TargetNode(super.schema, super.table, super.column);
	}

}