import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDDL {

    public static Connection connection = HBaseConnection.connection;

    /**
     * 创建命名空间
     * @param namespace 命名空间
     * @throws IOException 抛出
     */
    public static void createNameSpace(String namespace) throws IOException {
        Admin admin = connection.getAdmin();
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
        builder.addConfiguration("user", "atguigu");
        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查看表格是否存在
     * @param namespace 命名空间
     * @param tableName 表名
     * @return  是否存在
     * @throws IOException 抛出
     */
    public static boolean isTableExists(String namespace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        boolean b = false;
        try {
            b = admin.tableExists(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        admin.close();
        return b;
    }

    /**
     * 创建表
     * @param namespace 命名空间
     * @param tableName 表名
     * @param columnFamilies 表格描述
     * @throws IOException 抛出
     */
    public static void createTable(String namespace, String tableName, String... columnFamilies) throws IOException {

        if (columnFamilies.length == 0) {
            System.out.println("创建表格至少有一个列族");
            return;
        }
        if (isTableExists(namespace, tableName)) {
            System.out.println("表格已经存在");
            return;
        }

        Admin admin = connection.getAdmin();

        TableDescriptorBuilder tableDescriptorBuilder =
                TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));

        for (String columnFamily : columnFamilies) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
            columnFamilyDescriptorBuilder.setMaxVersions(3);
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }

        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("表格已经存在");
            e.printStackTrace();
        }
        admin.close();
    }

    /**
     * 修改表格中一个列族的版本
     * @param namespace 命名空间
     * @param tableName 表名
     * @param columnFamily  列族
     * @param version   版本
     * @throws IOException 抛出
     */
    public static void modifyTable(String namespace, String tableName, String columnFamily, int version) throws IOException {

        if (!isTableExists(namespace, tableName)) {
            System.out.println("表格不存在");
            return;
        }

        Admin admin = connection.getAdmin();

        try {
            TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName));
            ColumnFamilyDescriptor columnFamily1 = descriptor.getColumnFamily(Bytes.toBytes(columnFamily));

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);

            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(columnFamily1);
            columnFamilyDescriptorBuilder.setMaxVersions(3);

            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();
    }

    /**
     * 删除表格
     * @param namespace 命名空间名
     * @param tableName 表名
     * @return  是否成功
     */
    public static boolean deleteTable(String namespace, String tableName) throws IOException {
        if (!isTableExists(namespace, tableName)) {
            System.out.println("表格不存在，无法删除");
            return false;
        }

        Admin admin = connection.getAdmin();

        try {
            TableName tableName1 = TableName.valueOf(namespace, tableName);
            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        admin.close();
        return true;
    }

    public static void main(String[] args) throws IOException {
    }
}
