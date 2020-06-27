import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class HBaseService {
    private HBaseCon hBaseCon;
    private Connection connection;
    private HBaseCRUD crud;

    public HBaseService() {
        hBaseCon = new HBaseCon();
        connection = hBaseCon.getConnection();
        crud = new HBaseCRUD(connection);
    }


    public List<String> getSubDeptInfo(String tableName, String rowKey) {
        try {
            List<String> list = crud.toList(crud.get(tableName, rowKey, "subdept"));
            return list;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            hBaseCon.close();
        }
        return null;
    }

    public List<String> getTopDept(String tableName) {
        //设置单列过滤器
        Filter filter = new SingleColumnValueFilter("base".getBytes(),
                "parentdept".getBytes(),
                CompareFilter.CompareOp.EQUAL,
                "".getBytes());
        try {
            //开始查询
            List<String> list = crud.toList(crud.scan(tableName, filter));
            return list;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            hBaseCon.close();
        }
        return null;
    }

    public void putSubDept(String tableName, String deptRowKey, String newDeptRowKey, String newDeptName) {
        String s = "";
        String newSubDeptColumnName;
        try {
            //获取部门信息
            Cell[] cells = crud.get(tableName, deptRowKey, "subdept").rawCells();
            for (Cell cell : cells) {
                s = new String(CellUtil.cloneQualifier(cell));
                s = Pattern.compile("\\D").matcher(s).replaceAll("").trim();
            }

            //构造新子部门列名
            newSubDeptColumnName = "subdept" + (Integer.parseInt(s) + 1);
            //插入子部门信息
            crud.put(tableName, newDeptRowKey,
                    "base",
                    "name",
                    newDeptName);
            crud.put(tableName, newDeptRowKey,
                    "base",
                    "parentdept",
                    deptRowKey);
            //向父部门插入新子部门信息
            crud.put(tableName, deptRowKey,
                    "subdept",
                    newSubDeptColumnName,
                    newDeptName);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            hBaseCon.close();
        }
    }

    public void abandonedDept(String tableName, String deptRowKey, String newdeptRowKey) {
        String s = "";
        String newSubDeptColumnName;
        ArrayList<String> newDeptNameList = new ArrayList<String>();
        ArrayList<String> subDeptRKList = new ArrayList<String>();

        //查找下级部门的过滤器
        Filter filter = new SingleColumnValueFilter("base".getBytes(),
                "parentdept".getBytes(),
                CompareFilter.CompareOp.EQUAL,
                deptRowKey.getBytes());

        try {
            //获取部门下级部门rowkey
            ResultScanner resultScanner = crud.scan(tableName, filter, new String[]{"base"}, new String[]{"parentdept"});
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    //下级部门rk
                    String rk = new String(CellUtil.cloneRow(cell));
                    subDeptRKList.add(rk);
                    //修改所有下级部门的上级部门id
                    crud.put(tableName, rk, "base", "parentdept", newdeptRowKey);
                }
            }
            for (String rk : subDeptRKList) {
                //获取所有下级部门名称
                Cell[] cells = crud.get(tableName, rk, new String[]{"base"}, new String[]{"name"}).rawCells();
                for (Cell cell : cells) {
                    newDeptNameList.add(new String(CellUtil.cloneValue(cell)));
                }
            }

            //在新上级部门中添加新下级部门信息
            //获取新上级部门的下级部门信息
            Cell[] cells = crud.get(tableName, newdeptRowKey, "subdept").rawCells();
            for (Cell cell : cells) {
                //列名
                s = new String(CellUtil.cloneQualifier(cell));
                //提取列名排序
                s = Pattern.compile("\\D").matcher(s).replaceAll("").trim();
            }
            for (String newDeptName : newDeptNameList) {
                s = String.valueOf(Integer.parseInt(s) + 1);
                newSubDeptColumnName = "subdept" + s;
                crud.put(tableName, newdeptRowKey,
                        "subdept",
                        newSubDeptColumnName,
                        newDeptName);
            }

            //删除当前部门
            crud.delete(tableName, deptRowKey);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            hBaseCon.close();
        }
    }
}
