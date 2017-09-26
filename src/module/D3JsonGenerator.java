package module;

import java.sql.SQLException;
import java.util.List;
import java.util.Stack;

import util.DBUtil;
import util.DBUtil.DB_TYPE;
import bean.LineageTree;
import bean.NodeLine;
import bean.ResultLine;
import bean.SourceNode;
import bean.TargetNode;

/**
 * D3Json生成类
 * 
 * @author: hshe-161202
 * @create date: 2017年8月14日
 *
 */
public class D3JsonGenerator {

	private int lev = 0;
	private String etlPath;
	private String etlName;
	private DBUtil dbUtil = new DBUtil(DB_TYPE.RESULT);

	/**
	 * 定义栈parent，避免陷入循环关系，剔除已经添加过的节点
	 */
	private Stack<NodeLine> parent = new Stack<NodeLine>();
	
	
	public LineageTree Draw(NodeLine node) {
 		
		LineageTree tree = Draw(node, new LineageTree(null));
		
		dbUtil.close();
		
		return tree;
	}

	private LineageTree Draw(NodeLine node, LineageTree tree) {

		lev++;
		parent.push(node);
		
		DrawCurrent(node, tree);
		DrawChild(node, tree);

		lev--;
		//parent.pop();
		
		return tree;
	}

	private void DrawChild(NodeLine node, LineageTree tree) {

		if (node instanceof SourceNode) {
			List<ResultLine> sqlResList = null;
			try {
				sqlResList = dbUtil.doSelect((SourceNode) node);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (ResultLine sqlRes : sqlResList) {
				TargetNode sqlResNode = sqlRes.getTargetNode();
				etlPath = sqlRes.getEtlPath();
				etlName = sqlRes.getEtlName();
				
				LineageTree child = new LineageTree(sqlResNode.toString());
				
				if (!parent.contains(sqlResNode)) {
					child = Draw(sqlResNode.toSourceLine(), child);
					tree.addChild(child);
				}
			}
		} else if (node instanceof TargetNode) {
			List<ResultLine> sqlResList = null;
			try {
				sqlResList = dbUtil.doSelect((TargetNode) node);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (ResultLine sqlRes : sqlResList) {
				SourceNode sqlResNode = sqlRes.getSourceNode();
				etlPath = sqlRes.getEtlPath();
				etlName = sqlRes.getEtlName();

				LineageTree child = new LineageTree(sqlResNode.toString());
				
				if (!parent.contains(sqlResNode)) {
					child = Draw(sqlResNode.toTargetLine(), child);
					tree.addChild(child);
				}
			}
		}
	}

	private void DrawCurrent(NodeLine node, LineageTree tree) {
		tree.setName(node.toString());
	}
	

	/**
	 * main
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) {
		
 		long startTime = System.currentTimeMillis();
 		

		TargetNode node1 = new TargetNode("pwork", "lineage_test_sh", "km02avaflg");
		SourceNode node2 = new SourceNode("pdata", "t03_deposit_acct_base_info", "acct_id");
		TargetNode node3 = new TargetNode("pdata", "t00_exch_rate_info", "cnvt_exch_rate");
		SourceNode node4 = new SourceNode("pdata", "t00_exch_rate_info", "cnvt_exch_rate");
		SourceNode node5 = new SourceNode("shdata", "t99_std_cde_map_info", "targcde_cd");
		D3JsonGenerator drawLineage = new D3JsonGenerator();
		
		System.out.println(drawLineage.Draw(node4).toString());
		

		long endTime = System.currentTimeMillis();
		System.out.println("方法1查询完成!耗时 " + Float.toString((endTime - startTime) / 1000F) + " 秒");
	}
}
