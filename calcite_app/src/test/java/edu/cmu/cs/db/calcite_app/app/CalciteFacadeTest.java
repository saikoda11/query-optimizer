package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CalciteFacadeTest {
    private static CalciteFacade calciteFacade;
    private static List<String> sqls;
    private static List<Boolean> isExpectedToParse;
    private static List<Boolean> isExpectedToValidate;
    private static final String duckDbFIle = "../input/qop1.db";

    @BeforeAll
    static void init() {
        DataSource dataSource = JdbcSchema.dataSource(
                "jdbc:duckdb:" + duckDbFIle,
                "org.duckdb.DuckDBDriver",
                null,
                null);
        DatabaseFacade.init(dataSource);
        try {
            calciteFacade = new CalciteFacade();
        } catch (SQLException e) {
            fail(e);
        }
        sqls = List.of(
                "",
                "select * from lineitem",
                """
                select
                sum(l_extendedprice * l_discount) as revenue
                from
                lineitem
                where
                l_shipdate >= date '1995-01-01'
                and l_shipdate < date '1995-01-01' + interval '1' year
                and l_discount between 0.05 - 0.01 and 0.05 + 0.01
                and l_quantity < 24
                """,
                "select * from nonexistentrelation"
        );
        isExpectedToParse = List.of(false, true, true, true);
        isExpectedToValidate = List.of(false, true, true, false);
    }

    @Test
    void testConstructorSuccess() {
        assertDoesNotThrow(CalciteFacade::new);
    }

    @Test
    void testPipelineDoesNotThrow() {
        int successCount = 0;
        for (int i = 0; i < sqls.size(); ++i) {
            try {
                SqlNode sqlNode = calciteFacade.parse(sqls.get(i));
                sqlNode = calciteFacade.validate(sqlNode);
                RelNode relNode = calciteFacade.sql2rel(sqlNode);
                relNode = calciteFacade.optimize(relNode);
                successCount += 1;
            } catch (SqlParseException e) {
                if (isExpectedToParse.get(i)) {
                    fail(e);
                }
            } catch (CalciteContextException e) {
                if (isExpectedToValidate.get(i)) {
                    fail(e);
                }
            }
        }
        assert isExpectedToValidate.stream().mapToInt(i -> i ? 1 : 0).sum() == successCount;
    }

    @Test
    void testExecute() {
        try {
            SqlNode sqlNode = calciteFacade.parse(sqls.get(2));
            sqlNode = calciteFacade.validate(sqlNode);
            RelNode relNode = calciteFacade.sql2rel(sqlNode);
            relNode = calciteFacade.optimize(relNode);
            String sql = calciteFacade.rel2SqlString(relNode).getSql();
            calciteFacade.execute(sql, AppUtils.getResultSetSerializer());
        } catch (SqlParseException | SQLException | ClassNotFoundException e) {
            fail(e);
        }
    }
}
