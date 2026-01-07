package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.*;

import java.sql.*;
import java.util.Collections;
import java.util.function.Consumer;

public class CalciteFacade {
    private final CalciteConnectionConfig config = AppUtils.getCalciteConnectionConfig();
    private final RelDataTypeFactory typeFactory = CustomSchema.getTypeFactory();
    private final CustomSchema customSchema;

    private final SqlValidator validator;
    private final SqlToRelConverter converter;
    private final VolcanoPlanner planner;
    private final RelOptCluster cluster;

    public CalciteFacade() throws SQLException {
        customSchema = CustomSchema.create();
        this.validator = createValidator();

        planner = new VolcanoPlanner(
                RelOptCostImpl.FACTORY,
                Contexts.of(config)
        );

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        cluster = RelOptCluster.create(
                planner,
                new RexBuilder(typeFactory)
        );

        converter = createConverter();
    }

    private SqlValidator createValidator() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(customSchema.getSchemaName(), customSchema);
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema,
                Collections.singletonList(customSchema.getSchemaName()),
                typeFactory,
                config);


        SqlValidator.Config sqlValidatorConfig = AppUtils.getSqlValidatorConfig();
        return SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                sqlValidatorConfig
        );
    }

    private SqlToRelConverter createConverter() {
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);
        RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;
        return new SqlToRelConverter(
                NOOP_EXPANDER,
                validator,
                customSchema.getCatalogReader(),
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );
    }

    public SqlNode parse(String sql) throws SqlParseException {
        SqlParser.Config sqlParserConfig = AppUtils.getSqlParserConfig();
        SqlParser sqlParser = SqlParser.create(sql, sqlParserConfig);
        return sqlParser.parseQuery();
    }

    public SqlNode validate(SqlNode sqlNode) {
        return validator.validate(sqlNode);
    }

    public RelNode sql2rel(SqlNode sqlNode) {
        RelRoot root = converter.convertQuery(sqlNode, false, true);
        return root.rel;
    }

    public SqlString rel2SqlString(RelNode relNode) {
        RelToSqlConverter rel2sql = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
        RelToSqlConverter.Result res = rel2sql.visitRoot(relNode);
        SqlNode optimizedSqlNode = res.asQueryOrValues();
        return optimizedSqlNode.toSqlString(PostgresqlSqlDialect.DEFAULT);
    }

    public RelNode optimize(RelNode relNode) {
        RuleSet rules = RuleSets.ofList(
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE,
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_RULE,
                EnumerableRules.ENUMERABLE_FILTER_RULE,
                EnumerableRules.ENUMERABLE_CALC_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE
        );
        Program program = Programs.of(RuleSets.ofList(rules));

        cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);
        return program.run(
                planner,
                relNode,
                relNode.getTraitSet().plus(EnumerableConvention.INSTANCE),
                Collections.emptyList(),
                Collections.emptyList()
        );
    }

    public void execute(String sql, Consumer<ResultSet> serializer) throws SQLException, ClassNotFoundException {
        DatabaseFacade.getInstance().execute(sql, serializer);
    }
}
