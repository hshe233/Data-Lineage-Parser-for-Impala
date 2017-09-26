package bean;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;

/**
 * 血缘树
 * 
 * @author: hshe-161202
 * @create date: 2017年7月27日
 * 
 */
public class LineageTree {

	private String name;

	private List<LineageTree> children = new ArrayList<LineageTree>();

	private JSONObject jsonObj = new JSONObject();

	public LineageTree() {
	}

	public LineageTree(String str) {
		this.name = str;
	}
	
	/**
	 * 将对象转换成json文本
	 */
	@Override
	@SuppressWarnings("unchecked")
	public String toString() {
		jsonObj.put("name", this.name);
		jsonObj.put("branchLength", this.branchLength());
		jsonObj.put("leafAmount", this.leafAmount());
		if (this.hasChild())
			jsonObj.put("children", this.children);
		return jsonObj.toString();
	}

	/**
	 * 计算树的最长分支上的节点数
	 * @return
	 */
	public int branchLength() {
		int depth = 1;
		int max = 0;
		for (LineageTree child : this.children) {
			max = Math.max(child.branchLength(), max);
		}
		depth += max;
		return depth;
	}
	
	/**
	 * 计算叶子节点数
	 * @return
	 */
	public int leafAmount() {
		int leafNum = 1;
		if (this.hasChild()) {
			leafNum = 0;
			for (LineageTree child : this.children) {
				if (child.hasChild())
					leafNum += child.leafAmount();
				else
					leafNum ++;
			}
		}
		return leafNum;
	}

	public boolean hasChild() {
		return !this.children.isEmpty();
	}

	public void addChild(LineageTree tree) {
		children.add(tree);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<LineageTree> getChildren() {
		return children;
	}

	public void setChildren(List<LineageTree> children) {
		this.children = children;
	}

}
