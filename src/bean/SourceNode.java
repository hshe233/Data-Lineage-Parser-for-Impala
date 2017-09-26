package bean;

/**
 * Դ�ڵ�
 * 
 * @author: hshe-161202
 * @create date: 2017��7��24��
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