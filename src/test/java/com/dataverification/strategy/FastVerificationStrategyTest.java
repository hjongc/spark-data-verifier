package com.dataverification.strategy;

import com.dataverification.model.VerificationMode;
import com.dataverification.model.VerificationResult;
import com.dataverification.model.VerificationStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FastVerificationStrategyTest {

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Mock
    private ResultSet countResultSet;

    @Mock
    private ResultSet joinResultSet;

    @Mock
    private ResultSetMetaData metaData;

    private FastVerificationStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new FastVerificationStrategy();
    }

    @Test
    void testVerify_MatchingData_ReturnsOK() throws Exception {
        // Arrange
        List<String> columns = Arrays.asList("id", "name", "value");
        when(connection.createStatement()).thenReturn(statement);

        // Mock count query - matching counts
        when(statement.executeQuery(contains("COUNT(*)"))).thenReturn(countResultSet);
        when(countResultSet.next()).thenReturn(true);
        when(countResultSet.getLong("base_count")).thenReturn(100L);
        when(countResultSet.getLong("target_count")).thenReturn(100L);

        // Mock join query - no differences
        when(statement.executeQuery(contains("FULL OUTER JOIN"))).thenReturn(joinResultSet);
        when(joinResultSet.next()).thenReturn(false); // No differences
        when(joinResultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(3);

        // Act
        VerificationResult result = strategy.verify(
                connection, "base_db", "target_db", "test_table",
                columns, "1=1", 5
        );

        // Assert
        assertNotNull(result);
        assertEquals(VerificationStatus.OK, result.getStatus());
        assertEquals(100L, result.getBaseRowCount());
        assertEquals(100L, result.getTargetRowCount());
        assertEquals(0L, result.getDifferencesFound());
        assertEquals(VerificationMode.FAST, result.getMode());
    }

    @Test
    void testVerify_DifferentCounts_ReturnsNotOK() throws Exception {
        // Arrange
        List<String> columns = Arrays.asList("id", "name");
        when(connection.createStatement()).thenReturn(statement);

        // Mock count query - different counts
        when(statement.executeQuery(anyString())).thenReturn(countResultSet);
        when(countResultSet.next()).thenReturn(true);
        when(countResultSet.getLong("base_count")).thenReturn(100L);
        when(countResultSet.getLong("target_count")).thenReturn(95L);

        // Act
        VerificationResult result = strategy.verify(
                connection, "base_db", "target_db", "test_table",
                columns, "1=1", 5
        );

        // Assert
        assertNotNull(result);
        assertEquals(VerificationStatus.NOT_OK, result.getStatus());
        assertEquals(100L, result.getBaseRowCount());
        assertEquals(95L, result.getTargetRowCount());
        assertEquals(5L, result.getDifferencesFound());
        assertTrue(result.getMessage().contains("mismatch"));
    }

    @Test
    void testVerify_EmptyTables_ReturnsOK() throws Exception {
        // Arrange
        List<String> columns = Arrays.asList("id");
        when(connection.createStatement()).thenReturn(statement);

        // Mock count query - both empty
        when(statement.executeQuery(anyString())).thenReturn(countResultSet);
        when(countResultSet.next()).thenReturn(true);
        when(countResultSet.getLong("base_count")).thenReturn(0L);
        when(countResultSet.getLong("target_count")).thenReturn(0L);

        // Act
        VerificationResult result = strategy.verify(
                connection, "base_db", "target_db", "empty_table",
                columns, "1=1", 5
        );

        // Assert
        assertNotNull(result);
        assertEquals(VerificationStatus.OK, result.getStatus());
        assertEquals(0L, result.getBaseRowCount());
        assertEquals(0L, result.getTargetRowCount());
        assertTrue(result.getMessage().contains("empty"));
    }

    private ResultSet contains(String substring) {
        return argThat(sql -> sql != null && sql.toString().contains(substring));
    }
}
