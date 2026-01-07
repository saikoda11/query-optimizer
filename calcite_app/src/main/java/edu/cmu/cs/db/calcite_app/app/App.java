package edu.cmu.cs.db.calcite_app.app;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.Collections;
import java.util.List;

import com.github.vertical_blank.sqlformatter.SqlFormatter;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlString;

import javax.sql.DataSource;

public class App
{
    private static void SerializeSql(String sql, File outputPath) throws IOException {
        Files.writeString(outputPath.toPath(), sql);
    }

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
        String outputDir = args[0];
        String queriesDir = args[1];
        String dbFile = args[2];
        boolean isTest = Boolean.parseBoolean(args[3]);
        System.out.println("Running the app!");
        System.out.println("\toutputDir: " + outputDir);
        System.out.println("\tqueriesDir: " + queriesDir);
        System.out.println("\tdbFile: " + dbFile);
        System.out.println("\tisTest: " + isTest);

        if (!Path.of(dbFile).toFile().exists()) {
            System.out.println("DB File not found or does not exist");
            return;
        }
        DataSource datasource = JdbcSchema.dataSource(
                "jdbc:duckdb:" + dbFile, "org.duckdb.DuckDBDriver", null, null);
        DatabaseFacade.init(datasource);

        CalciteFacade calciteFacade = null;
        try {
            calciteFacade = new CalciteFacade();
        } catch (SQLException e) {
            System.out.println("Could not open db file: " + dbFile);
            return;
        }

        List<Path> orderedSqlPaths;
        if (isTest) {
            orderedSqlPaths = Collections.singletonList(Path.of("input/queries/q6.sql"));
        } else {
            orderedSqlPaths = InputDirectoryProcessor.processDir(queriesDir);
        }
        for (Path path : orderedSqlPaths) {
            String filename = getFileNameWithoutExtension(path.toFile());
            System.out.printf("FILE: %s\n", path);
            System.out.printf("filename without extension: %s\n", filename);
            String sql = "";
            try {
                sql = InputDirectoryProcessor.readSql(path);
                SerializeSql(sql, new File(outputDir + "/" + filename + ".sql"));

                SqlNode sqlNode = calciteFacade.parse(sql);
                sqlNode = calciteFacade.validate(sqlNode);
                RelNode relNode = calciteFacade.sql2rel(sqlNode);
                SerializePlan(relNode, new File(outputDir + "/" + filename + ".txt"));

                relNode = calciteFacade.optimize(relNode);
                SerializePlan(relNode, new File(outputDir + "/" + filename + "_optimized.txt"));

                SqlString optimizedSqlString = calciteFacade.rel2SqlString(relNode);
                String optimizeSql = optimizedSqlString.toString();
                SerializeSql(optimizeSql, new File(outputDir + "/" + filename + "_optimized.sql"));
            } catch (IOException e) {
                System.out.printf("IOException: %s\n", e.getMessage());
            } catch (SqlParseException e) {
                System.out.printf("ParseException: %s\nSQL: %s\n", e.getMessage(), SqlFormatter.format(sql));
            }

        }
    }

    private static String getFileNameWithoutExtension(File file) {
        String filename = file.getName();
        return filename.substring(0, filename.lastIndexOf("."));
    }
}
