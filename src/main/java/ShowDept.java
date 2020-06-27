import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

@WebServlet("/show")
public class ShowDept extends HttpServlet {

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        HBaseService service = new HBaseService();
        String action = request.getParameter("do");
        //判断操作
        if(action.equals("getTop")){
            //查询顶级部门
            List<String> list = service.getTopDept(request.getParameter("tableName"));
            request.setAttribute("tList",list);
            request.getRequestDispatcher("index.jsp").forward(request,response);
        } else if(action.equals("getSub")){
            //查询子部门
            List<String> list = service.getSubDeptInfo(request.getParameter("tableName"),request.getParameter("rowkey"));
            request.setAttribute("tList",list);
            request.getRequestDispatcher("index.jsp").forward(request,response);
        }
    }
}
