import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class PhoenixConnection {
    public static void main(String[] args) throws SQLException, IOException {
        // ��׼��jdbc
        // url
        String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

        // ���ö��� û���û���������
        Properties properties = new Properties();

        // ��ȡ����
        Connection connection = DriverManager.getConnection(url, properties);

        // ����sql ����д;
        PreparedStatement preparedStatement =
                connection.prepareStatement("select * from student");

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + ":" + resultSet.getString(2)
                    + " " + resultSet.getLong(3) + " " + resultSet.getString(4));
        }

        connection.close();

        // �ر�֮ǰд���൥������
//        HBaseConnection.closeConnection();

    }
}
