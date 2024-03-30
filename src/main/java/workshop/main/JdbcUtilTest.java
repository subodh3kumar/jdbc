package workshop.main;

import workshop.util.JdbcUtil;

import java.sql.Connection;

public class JdbcUtilTest {

    public static void main(String[] args) {
        Connection connection = JdbcUtil.getSourceConnection();
        if (connection != null) {
            System.out.println("connection created");
        }
    }
}
