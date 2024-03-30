package workshop.main;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import workshop.util.JdbcUtil;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteAsJsonObjectMapper {

    private static final String SELECT_SQL_QUERY = """
            SELECT * FROM ORA.DEPT
            """;

    public static void main(String[] args) {
        //List<Map<String, Object>> payloadList = new ArrayList<>();

        Path path = Path.of("src/main/resources/table.txt");

        try (Connection connection = JdbcUtil.getSourceConnection()) {
            Files.deleteIfExists(path);
            Writer writer = new FileWriter(path.toFile(), StandardCharsets.UTF_8, true);

            PreparedStatement ps = connection.prepareStatement(SELECT_SQL_QUERY);
            ResultSet rs = ps.executeQuery();

            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columnResultSet = metaData.getColumns(null, "ORA", "DEPT", null);

            List<String> columnList = new ArrayList<>();
            while (columnResultSet.next()) {
                String column = columnResultSet.getString("COLUMN_NAME");
                columnList.add(column);
            }

            String columnNames = String.join(", ", columnList);
            System.out.println("columns: " + columnNames);

            JsonFactory jsonFactory = new JsonFactory();
            jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET,false);
            ObjectMapper objectMapper = new ObjectMapper();

            while (rs.next()) {

                Map<String, Object> payload = new HashMap<>();
                for (String column : columnList) {
                    payload.put(column, rs.getObject(column));
                }
                objectMapper.writeValue(writer, payload);
                // payloadList.add(payload);
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
