import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseCRUD {
    private Connection connection;
    private Admin admin;

    public HBaseCRUD(Connection connection, Admin admin) {
        this.connection = connection;
        this.admin = admin;
    }

    public HBaseCRUD(Connection connection) {
        this.connection = connection;
    }

    public HBaseCRUD(Admin admin) {
        this.admin = admin;
    }

    public void createTable(String tableName, String... columnFamilyNames) throws IOException {
        TableName name = TableName.valueOf(tableName);
        if (!admin.tableExists(name)) {
            HTableDescriptor htd = new HTableDescriptor(name);
            for (String familyName : columnFamilyNames)
                htd.addFamily(new HColumnDescriptor(familyName));
            admin.createTable(htd);
        }
    }

    //put操作
    public void put(String tableName, String rowKey, String columnFamilyName, String columnName, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put p = new Put(rowKey.getBytes());
        p.addColumn(columnFamilyName.getBytes(), columnName.getBytes(), value.getBytes());
        table.put(p);
    }


    public ResultScanner scan(String tableName, Filter filter, String[] familyNames, String[] columnNames) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setFilter(filter);
        if (familyNames.length != columnNames.length) {
            return null;
        }
        for (int i = 0; i < familyNames.length; i++)
            scan.addColumn(familyNames[i].getBytes(), columnNames[i].getBytes());
        return table.getScanner(scan);
    }

    //带过滤器的扫描方法
    public ResultScanner scan(String tableName, Filter filter) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setFilter(filter);
        return table.getScanner(scan);
    }

    //行键和列族获取数据
    public Result get(String tableName, String rowKey, String familyName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addFamily(familyName.getBytes());
        return table.get(get);
    }

    public Result get(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        return table.get(get);
    }

    public Result get(String tableName, String rowKey, String[] familyNames, String[] columnNames) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        if (familyNames.length != columnNames.length) {
            return null;
        }
        for (int i = 0; i < familyNames.length; i++)
            get.addColumn(familyNames[i].getBytes(), columnNames[i].getBytes());
        return table.get(get);
    }

    public void delete(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
    }

    public List<String> toList(Result result) {
        ArrayList<String> list = new ArrayList<String>();
        if (result != null) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String s = "";
                s += "RowKey：" + new String(CellUtil.cloneRow(cell)) + "\t";
                s += "列族：" + new String(CellUtil.cloneFamily(cell)) + "\t";
                s += "列名：" + new String(CellUtil.cloneQualifier(cell)) + "\t";
                s += "值：" + new String(CellUtil.cloneValue(cell)) + "\t";
                list.add(s);
            }
        }
        return list;
    }

    public List<String> toList(ResultScanner resultScanner) {
        ArrayList<String> list = new ArrayList<String>();
        if (resultScanner != null) {
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    String s = "";
                    s += "RowKey：" + new String(CellUtil.cloneRow(cell)) + "\t";
                    s += "列族：" + new String(CellUtil.cloneFamily(cell)) + "\t";
                    s += "列名：" + new String(CellUtil.cloneQualifier(cell)) + "\t";
                    s += "值：" + new String(CellUtil.cloneValue(cell)) + "\t";
                    list.add(s);
                }
            }
        }
        return list;
    }
}
