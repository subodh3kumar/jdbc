package workshop.main;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import workshop.model.Column;
import workshop.util.JdbcUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WriteAsJsonWithoutAPI {
    private static final String FILE_PATH = "src/main/resources/table.txt";

    private static final String NULL_STR = "null";

    //http://www.java2s.com/example/java-api/java/sql/resultset/fetch_forward-0.html

    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);

        System.out.print("Enter table name: ");
        String tableName = input.nextLine();

        System.out.print("Enter where clause: ");
        String whereClause = input.nextLine();

        System.out.println("table name: " + tableName);
        System.out.println("where clause: " + whereClause);

        if (tableName.isBlank()) {
            throw new NullPointerException("table name can't be null");
        }

        List<Column> columns = new ArrayList<>();

        readSourceTableRows(tableName, whereClause, columns);
        insertTargetTableRows(tableName, whereClause, columns);
    }

    private static void insertTargetTableRows(String tableName, String whereClause, List<Column> columns) {
        Path path = Path.of(FILE_PATH);
        boolean exists = Files.exists(path);
        if (!exists) {
            throw new RuntimeException();
        }
        System.out.println("table rows file is available, file name: " + path.getFileName().toString());

        String insertQuery = "INSERT INTO DEV." + tableName + " VALUES (" + getPlaceholder(columns.size()) + ")";
        System.out.println("insert query: " + insertQuery);

        String deleteQuery = "DELETE FROM DEV." + tableName;
        if (!whereClause.isBlank()) {
            deleteQuery += " " + deleteQuery;
        }
        System.out.println("delete query: " + deleteQuery);

        try (Connection connection = JdbcUtil.getTargetConnection();
             PreparedStatement deleteStatement = connection.prepareStatement(deleteQuery);
             PreparedStatement insertStatement = connection.prepareStatement(insertQuery)) {

            int num = deleteStatement.executeUpdate();
            System.out.println("total rows deleted: " + num);

            ObjectMapper objectMapper = new ObjectMapper();
            Files.readAllLines(path).forEach(json -> {
                try {
                    Map<String, Object> map = objectMapper.readValue(json, new TypeReference<>() {
                    });
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        int index = Integer.parseInt(entry.getKey());
                        Object value = entry.getValue();

                        for (Column column : columns) {
                            if (column.columnIndex() == index) {
                                String type = column.columnType();
                                switch (type) {
                                    case "DATE" -> {
                                        if (NULL_STR.equals(value)) {
                                            insertStatement.setNull(index, Types.DATE);
                                        } else {
                                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                            java.util.Date parse = sdf.parse(value.toString().substring(0, 10));
                                            insertStatement.setObject(index, new Date(parse.getTime()), Types.DATE);
                                        }
                                    }
                                    case "NUMBER" -> {
                                        int decimalDigit = column.decimalDigits();
                                        if (decimalDigit == 0) {
                                            if (NULL_STR.equals(value)) {
                                                insertStatement.setNull(index, Types.INTEGER);
                                            } else {
                                                insertStatement.setObject(index, value, Types.INTEGER);
                                            }
                                        } else {
                                            if (NULL_STR.equals(value)) {
                                                insertStatement.setNull(index, Types.DOUBLE);
                                            } else {
                                                insertStatement.setObject(index, value, Types.DOUBLE);
                                            }
                                        }
                                    }
                                    case "VARCHAR2" -> {
                                        if (NULL_STR.equals(value)) {
                                            insertStatement.setNull(index, Types.VARCHAR);
                                        } else {
                                            insertStatement.setObject(index, value, Types.VARCHAR);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    insertStatement.addBatch();
                } catch (JsonProcessingException | SQLException | ParseException e) {
                    throw new RuntimeException(e);
                }
            });
            int[] insertedRows = insertStatement.executeBatch();
            System.out.println("total rows inserted: " + insertedRows.length);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getPlaceholder(int size) {
        return Stream.generate(() -> "?").limit(size).collect(Collectors.joining(", "));

    }

    private static void readSourceTableRows(String tableName, String whereClause, List<Column> columns) {
        String selectQuery = "SELECT * FROM ORA." + tableName;
        if (!whereClause.isBlank()) {
            selectQuery += " " + whereClause.trim();
        }
        System.out.println("select query: " + selectQuery);

        try (Connection connection = JdbcUtil.getSourceConnection()) {
            PreparedStatement ps = connection.prepareStatement(selectQuery);
            ResultSet rs = ps.executeQuery();

            Path path = Path.of(FILE_PATH);

            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columnResultSet = metaData.getColumns(null, "ORA", tableName, null);

            while (columnResultSet.next()) {
                String columnName = columnResultSet.getString("COLUMN_NAME");
                String columnType = columnResultSet.getString("TYPE_NAME");
                int columnIndex = columnResultSet.getInt("ORDINAL_POSITION");
                int decimalDigits = columnResultSet.getInt("DECIMAL_DIGITS");
                columns.add(new Column(columnIndex, columnName, columnType, decimalDigits));
            }

            Files.deleteIfExists(path);

            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append("{");
                int num = 1;
                for (Column column : columns) {
                    if (columns.size() == num) {
                        sb.append("\"").append(column.columnIndex()).append("\"")
                                .append(":").append("\"")
                                .append(rs.getObject(column.columnIndex())).append("\"");
                    } else {
                        sb.append("\"").append(column.columnIndex()).append("\"")
                                .append(":")
                                .append("\"").append(rs.getObject(column.columnIndex())).append("\"").append(",");
                    }
                    ++num;
                }
                sb.append("}");
                sb.append(System.lineSeparator());
            }
            Files.write(path, sb.toString().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            System.out.println("table rows fetched to file successfully");
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
