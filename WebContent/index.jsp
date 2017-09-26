<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<%@ page import="
	bean.NodeLine,
	bean.SourceNode,
	bean.TargetNode,
	util.FileUtil,
	bean.LineageTree,module.D3JsonGenerator"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html;charset=utf-8"/>
	<title>impala血缘分析查询</title>
    <link type="text/css" href="css/style.css" rel="stylesheet"></link>
    <link type="text/css" href="css/bootstrap.min.css" rel="stylesheet"></link>
    <link type="text/css" href="css/common.css" rel="stylesheet"></link>
    <script type="text/javascript" src="js/d3.js"></script>
    <script type="text/javascript" src="js/d3.layout.js"></script>
    <style type="text/css">
		.node circle {
		  cursor: pointer;
		  fill: #fff;
		  stroke: steelblue;
		  stroke-width: 1.5px;
		}
		
		.node text {
		  font-size: 12px;
		  font-family: 微软雅黑,"Helvetica Neue", Helvetica;
		}
		
		path.link {
		  fill: none;
		  stroke: #ccc;
		  stroke-width: 1.5px;
		}
    </style>
  </head>
  
  <body>
 	<%
 		long startTime = System.currentTimeMillis();

 	 	 		String schema = null;
 	 	 		String table = null;
 	 	 		String column = null;
 	 	 		String flag = null;
 	 	 		NodeLine node = null;
 	 	 		String jsonFileName = null;
 	 	 		String jsonFilePath = null;
 	 	 		String jsonFile = null;
 	 	 		
 	 	 		FileUtil fileUtil = new FileUtil();
 	 	 		D3JsonGenerator d3JsonGenerator = new D3JsonGenerator();

 	 	 		schema = request.getParameter("Schema");
 	 	 		table = request.getParameter("Table");
 	 	 		column = request.getParameter("Column");
 	 	 		flag = request.getParameter("Flag");

 	 	 		if (schema != null && table != null && column != null && flag != null) {
 	 	 			if (flag.equals("Source")) {
 	 	 				node = (SourceNode) new SourceNode(schema.toLowerCase(), table.toLowerCase(), column.toLowerCase());
 	 	 			} else {
 	 	 				node = (TargetNode) new TargetNode(schema.toLowerCase(), table.toLowerCase(), column.toLowerCase());
 	 	 			}

 	 	 			//获取WebRoot目录地址
 	 	 			jsonFilePath = new java.io.File(application.getRealPath(request.getRequestURI())).getParent();
 	 	 			
 	 	 			//定义json文件名
 	 	 			jsonFileName = "data/" + node.toString() + "-" + flag + ".json";
 	 		
 	 	 			//拼装json文件路径+地址
 	 	 			jsonFile = jsonFilePath + "/" + jsonFileName;

 	 	 			if (!FileUtil.exist(jsonFile)) {
 	 	 				
 	 	 				System.out.println("开始生成json文件：" + jsonFileName);

 	 	 				LineageTree tree = d3JsonGenerator.Draw(node);

 	 	 				fileUtil.createFile(jsonFile, tree.toString());
 	 	 			} else {
 	 	 				System.out.println("文件已经存在，跳过json生成步骤");
 	 	 			}

 	 	 			long endTime = System.currentTimeMillis();
 	 	 			System.out.println("查询完成!耗时 " + Float.toString((endTime - startTime) / 1000F) + " 秒");
 	 	 		}
 	%>
 	
  	<div id="body">
      <div id="footer">
		<form class="form-horizontal" id="input" role="form">     
	        <label>请输入查询参数</label>
	        <input type="text" class="form-control" name="Schema" placeholder="Schema" required>	        
	        <label></label>
	        <input type="text" class="form-control" name="Table" placeholder="Table" required>	        
	        <label></label>
	        <input type="text" class="form-control" name="Column" placeholder="Column" required>	        
	        <label></label>
	        <select type="list" class="form-control" name="Flag" placeholder="Flag" required>
	        	<option>Source</option>
	        	<option>Target</option>
	        </select>
	        <label></label>
	        <button id="submit" type="submit" class="btn btn-default">查询</button>
	    </form>
      </div>
    </div>
    <script type="text/javascript">

	var m,w,h,i;   
	var root;
	var tree;
	var diagonal;
	var vis;
	
	d3.json("<%=jsonFileName%>", function(json) {
		
		root = json;
	
		m = [200, 350, 200, 350],
		w = root.branchLength * 350,
		h = root.leafAmount * 3.5 + 400,
		i = 0,
		
		root.x0 = 150;
		root.y0 = 200;
	
		tree = d3.layout.tree()
		    .size([h, w]);
	
		diagonal = d3.svg.diagonal()
		    .projection(function(d) { return [d.y, d.x]; });
	
		vis = d3.select("body").append("svg:svg")
		    .attr("width", w + m[1] + m[3])
		    .attr("height", h + m[0] + m[2])
		  	.append("svg:g")
		    .attr("transform", "translate(" + m[3] + "," + m[0] + ")");
	  
		function toggleAll(d) {
			if (d.children) {
				d.children.forEach(toggleAll);
				toggle(d);
			}
		}
	
		// Initialize the display to show a few nodes.
		toggleAll(root);
		toggle(root);
	  
		update(root);
	  
	});
	
	function update(source) {
		var duration = d3.event && d3.event.altKey ? 5000 : 500;
	
		// Compute the new tree layout.
		var nodes = tree.nodes(root).reverse();
	
		// Normalize for fixed-depth.
		nodes.forEach(function(d) { d.y = d.depth * 350; });
	
		// Update the nodes…
		var node = vis.selectAll("g.node")
			.data(nodes, function(d) { return d.id || (d.id = ++i); });
	
		// Enter any new nodes at the parent's previous position.
		var nodeEnter = node.enter().append("svg:g")
			.attr("class", "node")
	      	.attr("transform", function(d) { return "translate(" + source.y0 + "," + source.x0 + ")"; })
	      	.on("click", function(d) { toggle(d); update(d); });
	
		nodeEnter.append("svg:circle")
	      	.attr("r", 1e-6)
	      	.style("fill", function(d) { return d._children ? "lightsteelblue" : "#fff"; });
	
		nodeEnter.append("svg:text")
	      	.attr("x", function(d) { return d.children || d._children ? -10 : 10; })
	      	.attr("dy", ".35em")
	      	.attr("text-anchor", function(d) { return d.children || d._children ? "end" : "start"; })
	      	.text(function(d) { return d.name; })
	      	.style("fill-opacity", 1e-6);
	
		// Transition nodes to their new position.
		var nodeUpdate = node.transition()
	      	.duration(duration)
	      	.attr("transform", function(d) { return "translate(" + d.y + "," + d.x + ")"; });
	
		nodeUpdate.select("circle")
	      	.attr("r", 4.5)
	      	.style("fill", function(d) { return d._children ? "lightsteelblue" : "#fff"; });
	
		nodeUpdate.select("text")
			.style("fill-opacity", 1);
	
		// Transition exiting nodes to the parent's new position.
		var nodeExit = node.exit().transition()
			.duration(duration)
			.attr("transform", function(d) { return "translate(" + source.y + "," + source.x + ")"; })
			.remove();
	
		nodeExit.select("circle")
			.attr("r", 1e-6);
	
		nodeExit.select("text")
			.style("fill-opacity", 1e-6);
	
		// Update the links…
		var link = vis.selectAll("path.link")
			.data(tree.links(nodes), function(d) { return d.target.id; });
	
		// Enter any new links at the parent's previous position.
		link.enter().insert("svg:path", "g")
			.attr("class", "link")
			.attr("d", function(d) {
				var o = {x: source.x0, y: source.y0};
				return diagonal({source: o, target: o});
			})
			.transition()
			.duration(duration)
			.attr("d", diagonal);
	
		// Transition links to their new position.
		link.transition()
			.duration(duration)
			.attr("d", diagonal);
	
		// Transition exiting nodes to the parent's new position.
		link.exit().transition()
			.duration(duration)
			.attr("d", function(d) {
				var o = {x: source.x, y: source.y};
				return diagonal({source: o, target: o});
			})
			.remove();
	
		// Stash the old positions for transition.
		nodes.forEach(function(d) {
			d.x0 = d.x;
			d.y0 = d.y;
		});
	}
	
	// Toggle children.
	function toggle(d) {
	  if (d.children) {
	    d._children = d.children;
	    d.children = null;
	  } else {
	    d.children = d._children;
	    d._children = null;
	  }
	}

    </script>
  </body>
</html>
