package workshop.main;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import workshop.model.Column;
import workshop.util.JdbcUtil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReadWriteAsJson {
    private static final String FILE_PATH = "src/main/resources/data/";

    private static final String NULL_STR = "null";

    //http://www.java2s.com/example/java-api/java/sql/resultset/fetch_forward-0.html

    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);

        System.out.print("Enter table name: ");
        String tableName = input.nextLine();

        System.out.print("Enter where clause: ");
        String whereClause = input.nextLine();

        if (tableName.isBlank()) {
            throw new NullPointerException("table name can't be null");
        }

        deleteOldFilesIfExists();

        List<Column> columns = new ArrayList<>();
        String fileName = getFileName();

        readSourceTableRows(tableName, whereClause, columns, fileName);
        insertTargetTableRows(tableName, whereClause, columns, fileName);
    }

    private static String getFileName() {
        return FILE_PATH + "table_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"));
    }

    private static void deleteOldFilesIfExists() {
        try (Stream<Path> directoryWalk = Files.walk(Path.of(FILE_PATH))) {
            directoryWalk.map(Path::toFile)
                    .filter(Predicate.not(File::isDirectory))
                    .forEach(File::delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void insertTargetTableRows(String tableName, String whereClause, List<Column> columns, String fileName) {
        Path path = Path.of(fileName);
        verifyFilesExists(path);

        System.out.println("table rows file is available, file name: " + path.getFileName().toString());

        String insertQuery = "INSERT INTO DEV." + tableName + " VALUES (" + getPlaceholder(columns.size()) + ")";

        String deleteQuery = "DELETE FROM DEV." + tableName;
        if (!whereClause.isBlank()) {
            deleteQuery += " " + deleteQuery;
        }

        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        insertRows(lines, columns, insertQuery, deleteQuery);
    }

    private static void insertRows(List<String> list, List<Column> columns, String insertQuery, String deleteQuery) {
        try (Connection connection = JdbcUtil.getTargetConnection();
             PreparedStatement deleteStatement = connection.prepareStatement(deleteQuery);
             PreparedStatement insertStatement = connection.prepareStatement(insertQuery)) {

            int num = deleteStatement.executeUpdate();
            System.out.println("total rows deleted: " + num);

            ObjectMapper objectMapper = new ObjectMapper();
            list.forEach(json -> {
                try {
                    Map<String, Object> map = getJsonValue(json, objectMapper); //objectMapper.readValue(json, new TypeReference<>() {
                    //});
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        String columnName = entry.getKey();
                        Object value = entry.getValue();

                        for (Column column : columns) {
                            if (columnName.equals(column.columnName())) {
                                int index = column.columnIndex();
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
                                    case "TIMESTAMP(6)" -> {
                                        if (NULL_STR.equals(value)) {
                                            insertStatement.setNull(index, Types.DATE);
                                        } else {
                                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                                            java.util.Date parse = sdf.parse(value.toString());
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
                } catch (SQLException | ParseException e) {
                    throw new RuntimeException(e);
                }
            });
            int[] insertedRows = insertStatement.executeBatch();
            System.out.println("total rows inserted: " + insertedRows.length);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, Object> getJsonValue(String line, ObjectMapper objectMapper) {
        try {
            return objectMapper.readValue(line, new TypeReference<>() {
            });
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static void verifyFilesExists(Path path) {
        boolean exists = Files.exists(path);
        if (!exists) {
            throw new RuntimeException();
        }
    }

    private static String getPlaceholder(int size) {
        return Stream.generate(() -> "?").limit(size).collect(Collectors.joining(", "));
    }

    private static void readSourceTableRows(String tableName, String whereClause, List<Column> columns, String fileName) {
        String selectQuery = "SELECT * FROM ORA." + tableName;
        if (!whereClause.isBlank()) {
            selectQuery += " " + whereClause.trim();
        }

        Path path = Path.of(fileName);
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
             Connection connection = JdbcUtil.getSourceConnection()) {

            System.out.println("select query: " + selectQuery);

            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columnResultSet = metaData.getColumns(null, "ORA", tableName, null);

            while (columnResultSet.next()) {
                String columnName = columnResultSet.getString("COLUMN_NAME");
                String columnType = columnResultSet.getString("TYPE_NAME");
                int columnIndex = columnResultSet.getInt("ORDINAL_POSITION");
                int decimalDigits = columnResultSet.getInt("DECIMAL_DIGITS");
                columns.add(new Column(columnIndex, columnName, columnType, decimalDigits));
            }

            PreparedStatement statement = connection.prepareStatement(selectQuery);
            ResultSet rs = statement.executeQuery();

            while (rs.next()) {
                int num = 1;

                StringBuilder sb = new StringBuilder();
                sb.append("{");

                for (Column column : columns) {
                    if (columns.size() == num) {
                        sb.append("\"").append(column.columnName()).append("\"")
                                .append(":").append("\"")
                                .append(rs.getObject(column.columnIndex())).append("\"");
                    } else {
                        sb.append("\"").append(column.columnName()).append("\"")
                                .append(":")
                                .append("\"").append(rs.getObject(column.columnIndex())).append("\"").append(",");
                    }
                    ++num;
                }
                sb.append("}");
                sb.append(System.lineSeparator());
                writer.write(sb.toString());
                writer.flush();
            }
            System.out.println("table rows fetched to file successfully");
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
