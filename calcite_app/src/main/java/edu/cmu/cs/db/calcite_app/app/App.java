package edu.cmu.cs.db.calcite_app.app;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.Collections;
import java.util.List;

import com.github.vertical_blank.sqlformatter.SqlFormatter;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

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
        String outputDir = args[0];
        System.out.println("\toutputDir: " + outputDir);
        String queriesDir = args[1];
        System.out.println("\tqueriesDir: " + queriesDir);
        String dbFile = args[2];
        System.out.println("\tdbFile: " + dbFile);
        boolean isTest = Boolean.parseBoolean(args[3]);
        System.out.println("\tisTest: " + isTest);

        CalciteFacade calciteFacade = null;
        try {
            calciteFacade = new CalciteFacade(dbFile);
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
            System.out.printf("FILE: %s\n", path);
            String sql = "";
            try {
                sql = InputDirectoryProcessor.readSql(path);
                SqlNode sqlNode = calciteFacade.parse(sql);
                sqlNode = calciteFacade.validate(sqlNode);
                RelNode relNode = calciteFacade.sql2rel(sqlNode);
                System.out.println(
                        RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES)
                );

                relNode = calciteFacade.optimize(relNode);
                System.out.println(
                        RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES)
                );
            } catch (IOException e) {
                System.out.printf("IOException: %s\n", e.getMessage());
            } catch (SqlParseException e) {
                System.out.printf("ParseException: %s\nSQL: %s\n", e.getMessage(), SqlFormatter.format(sql));
            }

        }
    }
}
