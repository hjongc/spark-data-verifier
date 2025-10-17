package com.dataverification.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Utility class for converting ResultSet to various formats.
 */
public class ResultSetConverter {
    private static final Logger logger = LoggerFactory.getLogger(ResultSetConverter.class);

    /**
     * Convert ResultSet to list of tab-separated string rows.
     */
    public static List<String> convertToStringList(ResultSet rs) throws SQLException {
        List<String> results = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            StringJoiner rowJoiner = new StringJoiner("\t");

            for (int i = 1; i <= columnCount; i++) {
                String value;
                int columnType = metaData.getColumnType(i);

                // Handle BLOB/BINARY types specially
                if (columnType == Types.BLOB || columnType == Types.BINARY ||
                        columnType == Types.VARBINARY || columnType == Types.LONGVARBINARY) {
                    value = "[BLOB]";
                } else {
                    value = rs.getString(i);
                    if (value == null) {
                        value = "[NULL]";
                    }
                }

                rowJoiner.add(value);
            }

            results.add(rowJoiner.toString());
        }

        return results;
    }

    /**
     * Get column names from ResultSet.
     */
    public static List<String> getColumnNames(ResultSetMetaData metaData) throws SQLException {
        List<String> columnNames = new ArrayList<>();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            columnNames.add(metaData.getColumnName(i));
        }

        return columnNames;
    }
}
