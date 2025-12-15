package edu.cmu.cs.db.calcite_app.app;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.List;

import com.github.vertical_blank.sqlformatter.SqlFormatter;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

import javax.sql.DataSource;

public class App
{
    private static void SerializePlan(RelNode relNode, File outputPath) throws IOException {
        Files.writeString(outputPath.toPath(), RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
    }

    private static void SerializeResultSet(ResultSet resultSet, File outputPath) throws SQLException, IOException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder resultSetString = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) {
                resultSetString.append(",");
            }
            resultSetString.append(metaData.getColumnName(i));
        }
        resultSetString.append("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    resultSetString.append(",");
                }
                String s = resultSet.getString(i);
                s = s.replace("\n", "\\n");
                s = s.replace("\r", "\\r");
                s = s.replace("\"", "\"\"");
                resultSetString.append("\"");
                resultSetString.append(s);
                resultSetString.append("\"");
            }
            resultSetString.append("\n");
        }
        Files.writeString(outputPath.toPath(), resultSetString.toString());
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: java -jar App.jar <arg1> <arg2> <arg3>");
            return;
        }

        // Feel free to modify this to take as many or as few arguments as you want.
        System.out.println("Running the app!");
        String arg1 = args[0];
        System.out.println("\tArg1: " + arg1);
        String arg2 = args[1];
        System.out.println("\tArg2: " + arg2);
        String arg3 = args[2];
        System.out.println("\tArg3: " + arg3);

        CalciteSchema calciteSchema = createSchema(arg3);
        CalciteFacade calciteFacade = new CalciteFacade(calciteSchema);

        List<Path> orderedSqlPaths = InputDirectoryProcessor.processDir(arg2);
        for (Path path : orderedSqlPaths) {
            System.out.printf("FILE: %s\n", path);
            String sql = "";
            try {
                sql = InputDirectoryProcessor.readSql(path);
                SqlNode sqlNode = calciteFacade.validate(sql);
            } catch (IOException e) {
                System.out.printf("IOException: %s\n", e.getMessage());
            } catch (SqlParseException e) {
                System.out.printf("ParseException: %s\nSQL: %s\n", e.getMessage(), SqlFormatter.format(sql));
            }
        }

        // Note: in practice, you would probably use org.apache.calcite.tools.Frameworks.
        // That package provides simple defaults that make it easier to configure Calcite.
        // But there's a lot of magic happening there; since this is an educational project,
        // we guide you towards the explicit method in the writeup.
    }

    private static CalciteSchema createSchema(String duckDbFIlePath) {
        CalciteSchema calciteSchema = CalciteSchema.createRootSchema(false, false);
        DataSource datasource = JdbcSchema.dataSource(
                "jdbc:duckdb:" + duckDbFIlePath, "org.duckdb.DuckDBDriver", null, null);
        Schema schema = JdbcSchema.create(calciteSchema.plus(), "default", datasource, null, null);
        for (String tableName : schema.getTableNames()) {
            Table table = schema.getTable(tableName);
            calciteSchema.add(tableName, table);
        }
        return calciteSchema;
    }
}
