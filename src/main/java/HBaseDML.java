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
     * ��������
     * @param namespace �����ռ���
     * @param tableName ����
     * @param rowKey    ����
     * @param columnFamilies    ������
     * @param columnName    ����
     * @param value ֵ
     */
    public static void putCell(String namespace, String tableName, String rowKey,
                               String columnFamilies, String columnName, String value) throws IOException {

        if (!HBaseDDL.isTableExists(namespace, tableName)) {
            System.out.println("��񲻴���");
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
     * ��ȡ����
     * @param namespace �����ռ���
     * @param tableName ����
     * @param rowKey    ����
     * @param columnFamilies    ������
     * @param columnName    ����
     */
    public static void getCell(String namespace, String tableName, String rowKey,
                               String columnFamilies, String columnName) throws IOException {

        if (!HBaseDDL.isTableExists(namespace, tableName)) {
            System.out.println("��񲻴���");
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
     * ɨ������
     * @param namespace �����ռ���
     * @param tableName ����
     * @param startRow ��ʼ��row
     * @param stopRow   ������row
     */
    public static void scanCell(String namespace, String tableName,
                                String startRow, String stopRow) throws IOException {

        if (!HBaseDDL.isTableExists(namespace, tableName)) {
            System.out.println("��񲻴���");
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
     * ����ɨ������
     * @param namespace �����ռ���
     * @param tableName ����
     * @param startRow ��ʼ��row
     * @param stopRow   ������row
     * @param columnFamily ������
     * @param columnName    ����
     * @param value ֵ
     * @throws IOException �׳�
     */
    public static void filterCell(String namespace, String tableName,
                                String startRow, String stopRow,
                                  String columnFamily, String columnName, String value) throws IOException {

        if (!HBaseDDL.isTableExists(namespace, tableName)) {
            System.out.println("��񲻴���");
            return;
        }

        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));

        // ������ �����У�
        FilterList filterList = new FilterList();
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                // ������
                Bytes.toBytes(columnFamily),
                // ����
                Bytes.toBytes(columnName),
                // �ȽϹ�ϵ (��value�Ƚϣ���ͬ�����ڣ�С�ڵ�)
                CompareOperator.EQUAL,
                Bytes.toBytes(value)
        );

        // ������ ��ȫ���У�
        // �����ͬʱ����û�е�ǰ�е�����
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                // ������
                Bytes.toBytes(columnFamily),
                // ����
                Bytes.toBytes(columnName),
                // �ȽϹ�ϵ (��value�Ƚϣ���ͬ�����ڣ�С�ڵ�)
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
     * ɾ��һ���е�һ������
     * @param namespace �����ռ�
     * @param tableName ����
     * @param rowKey    ����
     * @param columnFamily  ����
     * @param columnName    ����
     */
    public static void deleteCell(String namespace, String tableName, String rowKey,
                                     String columnFamily, String columnName) throws IOException {

        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // addColumnɾ��һ���汾
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        // addColumnsɾ�����а汾
        // ���߼���Ҫɾ�����а汾������
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
