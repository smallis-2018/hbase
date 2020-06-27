<%@ page import="java.util.List" %>

<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
  <head>
    <title>Hbase实验四</title>
  </head>
  <body>

  <%
    List<String> List = (List<String>) request.getAttribute("tList");
  %>
  <div style="float:left;margin-left: 25px;margin-right: 25px;">
      <p>获取顶级部门信息</p>
      <form action="show?do=getTop" method="post">
        <p>要查询的表名：</p>
        <p><input name="tableName" type="text"/></p>
        <button type="submit">查询</button>
      </form>
  </div>
  <div style="float:left;margin-left: 25px;margin-right: 25px;">
      <p>获子部门信息</p>
      <form action="show?do=getSub" method="post">
        <p>要查询的表名：</p>
        <p><input name="tableName" type="text"/></p>
        <p>要查询的部门ID：</p>
        <p><input name="rowkey" type="text"/></p>
        <button type="submit">查询</button>
      </form>
    </div>
  <div style="float: right;">
      <ol>
          <%if (List!=null){
              for(String s : List){
          %>
          <li><%out.print(s);%></li>
          <%}
          }%>
      </ol>
  </div>
  </body>
</html>
