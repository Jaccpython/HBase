import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class PhoenixConnection {
    public static void main(String[] args) throws SQLException, IOException {
        // 标准的jdbc
        // url
        String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

        // 配置对象 没有用户名和密码
        Properties properties = new Properties();

        // 获取连接
        Connection connection = DriverManager.getConnection(url, properties);

        // 编译sql 不能写;
        PreparedStatement preparedStatement =
                connection.prepareStatement("select * from student");

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + ":" + resultSet.getString(2)
                    + " " + resultSet.getLong(3) + " " + resultSet.getString(4));
        }

        connection.close();

        // 关闭之前写的类单例连接
//        HBaseConnection.closeConnection();

    }
}
