package bean;

/**
 * Ŀ��ڵ�
 * 
 * @author: hshe-161202
 * @create date: 2017��7��24��
 * 
 */
public class TargetNode extends NodeLine {

	public TargetNode() {
		super();
	}
	
	public TargetNode(String name) {
		super(name);
	}

	public TargetNode(String schema, String table, String column) {
		super(schema, table, column);
	}

	public SourceNode toSourceLine() {
		return new SourceNode(super.schema, super.table, super.column);
	}

}
