import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConnection {

    public static Connection connection = null;
    static {
        try {
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void closeConnection() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println(HBaseConnection.connection);
        HBaseConnection.closeConnection();
    }
}
