import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDDL {

    public static Connection connection = HBaseConnection.connection;

    /**
     * ���������ռ�
     * @param namespace �����ռ�
     * @throws IOException �׳�
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
     * �鿴����Ƿ����
     * @param namespace �����ռ�
     * @param tableName ����
     * @return  �Ƿ����
     * @throws IOException �׳�
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
     * ������
     * @param namespace �����ռ�
     * @param tableName ����
     * @param columnFamilies �������
     * @throws IOException �׳�
     */
    public static void createTable(String namespace, String tableName, String... columnFamilies) throws IOException {

        if (columnFamilies.length == 0) {
            System.out.println("�������������һ������");
            return;
        }
        if (isTableExists(namespace, tableName)) {
            System.out.println("����Ѿ�����");
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
            System.out.println("����Ѿ�����");
            e.printStackTrace();
        }
        admin.close();
    }

    /**
     * �޸ı����һ������İ汾
     * @param namespace �����ռ�
     * @param tableName ����
     * @param columnFamily  ����
     * @param version   �汾
     * @throws IOException �׳�
     */
    public static void modifyTable(String namespace, String tableName, String columnFamily, int version) throws IOException {

        if (!isTableExists(namespace, tableName)) {
            System.out.println("��񲻴���");
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
     * ɾ�����
     * @param namespace �����ռ���
     * @param tableName ����
     * @return  �Ƿ�ɹ�
     */
    public static boolean deleteTable(String namespace, String tableName) throws IOException {
        if (!isTableExists(namespace, tableName)) {
            System.out.println("��񲻴��ڣ��޷�ɾ��");
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
