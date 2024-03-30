package workshop.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public final class JdbcUtil {

    private static Connection sourceConnection = null;
    private static Connection targetConnection = null;

    public static Connection getSourceConnection() {
        if (sourceConnection == null) {
            try {
                sourceConnection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/XEPDB1", "ORA", "root");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return sourceConnection;
    }

    public static Connection getTargetConnection() {
        if (targetConnection == null) {
            try {
                targetConnection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/XEPDB1", "DEV", "root");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return targetConnection;
    }
}
