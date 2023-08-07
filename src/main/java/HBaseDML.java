import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDML {

    public static Connection connection = HBaseConnection.connection;

    /**
     * 插入数据
     * @param namespace 命名空间名
     * @param tableName 表名
     * @param rowKey    主键
     * @param columnFamilies    列族名
     * @param columnName    列名
     * @param value 值
     */
    public static void putCell(String namespace, String tableName, String rowKey,
                               String columnFamilies, String columnName, String value) throws IOException {

        if (!HBaseDDL.isTableExists(namespace, tableName)) {
            System.out.println("表格不存在");
            return;
        }

        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamilies), Bytes.toBytes(columnName), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    /**
     * 读取数据
     * @param namespace 命名空间名
     * @param tableName 表名
     * @param rowKey    主键
     * @param columnFamilies    列族名
     * @param columnName    列名
     */
    public static void getCell(String namespace, String tableName, String rowKey,
                               String columnFamilies, String columnName) throws IOException {

        if (!HBaseDDL.isTableExists(namespace, tableName)) {
            System.out.println("表格不存在");
            return;
        }

        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamilies), Bytes.toBytes(columnName));
        get.readAllVersions();

        try {
            Result result = table.get(get);
            Cell[] cells = result.rawCells();

            for (Cell cell : cells) {
                System.out.println(new String(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
    }

    /**
     * 扫描数据
     * @param namespace 命名空间名
     * @param tableName 表名
     * @param startRow 开始的row
     * @param stopRow   结束的row
     */
    public static void scanCell(String namespace, String tableName,
                                String startRow, String stopRow) throws IOException {

        if (!HBaseDDL.isTableExists(namespace, tableName)) {
            System.out.println("表格不存在");
            return;
        }

        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));
        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.print(new String(CellUtil.cloneRow(cell))
                            + "-" + new String(CellUtil.cloneQualifier(cell))
                            + "-" + new String(CellUtil.cloneFamily(cell))
                            + "-" + new String(CellUtil.cloneValue(cell))
                            + "-" + "\t");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    /**
     * 过滤扫描数据
     * @param namespace 命名空间名
     * @param tableName 表名
     * @param startRow 开始的row
     * @param stopRow   结束的row
     * @param columnFamily 列族名
     * @param columnName    列名
     * @param value 值
     * @throws IOException 抛出
     */
    public static void filterCell(String namespace, String tableName,
                                String startRow, String stopRow,
                                  String columnFamily, String columnName, String value) throws IOException {

        if (!HBaseDDL.isTableExists(namespace, tableName)) {
            System.out.println("表格不存在");
            return;
        }

        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));

        // 过滤器 （单列）
        FilterList filterList = new FilterList();
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                // 列族名
                Bytes.toBytes(columnFamily),
                // 列名
                Bytes.toBytes(columnName),
                // 比较关系 (和value比较，相同，大于，小于等)
                CompareOperator.EQUAL,
                Bytes.toBytes(value)
        );

        // 过滤器 （全部列）
        // 结果会同时保留没有当前列的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                // 列族名
                Bytes.toBytes(columnFamily),
                // 列名
                Bytes.toBytes(columnName),
                // 比较关系 (和value比较，相同，大于，小于等)
                CompareOperator.EQUAL,
                Bytes.toBytes(value)
        );

        filterList.addFilter(columnValueFilter);

        scan.setFilter(filterList);
        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.print(new String(CellUtil.cloneRow(cell))
                            + "-" + new String(CellUtil.cloneQualifier(cell))
                            + "-" + new String(CellUtil.cloneFamily(cell))
                            + "-" + new String(CellUtil.cloneValue(cell))
                            + "-" + "\t");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    /**
     * 删除一行中的一列数据
     * @param namespace 命名空间
     * @param tableName 表名
     * @param rowKey    主键
     * @param columnFamily  列组
     * @param columnName    列名
     */
    public static void deleteCell(String namespace, String tableName, String rowKey,
                                     String columnFamily, String columnName) throws IOException {

        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // addColumn删除一个版本
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        // addColumns删除所有版本
        // 按逻辑需要删除所有版本的数据
        delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    public static void main(String[] args) {
    }
}
