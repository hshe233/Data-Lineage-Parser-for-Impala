package bean;

/**
 * 单节点
 * 
 * @author: hshe-161202
 * @create date: 2017年7月28日
 * 
 */
public class NodeLine {

	protected String schema;

	protected String table;

	protected String column;

	public NodeLine() {

	}

	public NodeLine(String name) {
		this.schema = name.split("\\.")[0];
		this.table = name.split("\\.")[1];
		this.column = name.split("\\.")[2];
	}

	public NodeLine(String schema, String table, String column) {
		
		this.schema = schema == null ? "" : schema;
		this.table = table == null ? "" : table;
		this.column = column == null ? "" : column;
	}

	public boolean equals(NodeLine node) {
		return equals((Object) node);
	}

	/**
	 * 重写比较函数，忽略大小写
	 */
	@Override
	public boolean equals(Object node) {
		if (this.toString().equalsIgnoreCase(((NodeLine) node).toString()))
			return true;
		else
			return false;
	}

	public String toString() {
		return this.schema + "." + this.table + "." + this.column;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

}
