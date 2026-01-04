package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.schema.Statistic;

public class CustomTableStatistic implements Statistic {
    private final long rowCount;

    public CustomTableStatistic(long rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public Double getRowCount() {
        return (double) rowCount;
    }
}
