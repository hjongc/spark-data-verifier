package com.dataverification.service;

import com.dataverification.model.TableMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TableMetadataServiceTest {

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Mock
    private ResultSet resultSet;

    private TableMetadataService service;

    @BeforeEach
    void setUp() {
        service = new TableMetadataService();
    }

    @Test
    void testAnalyzeTable_NonPartitioned() throws SQLException {
        // Arrange
        when(connection.createStatement()).thenReturn(statement);

        // Mock SHOW COLUMNS
        when(statement.executeQuery(contains("SHOW COLUMNS"))).thenReturn(resultSet);
        when(resultSet.next())
                .thenReturn(true) // id
                .thenReturn(true) // name
                .thenReturn(true) // value
                .thenReturn(true) // updated_at
                .thenReturn(false);
        when(resultSet.getString(1))
                .thenReturn("id")
                .thenReturn("name")
                .thenReturn("value")
                .thenReturn("updated_at");

        // Mock SHOW PARTITIONS - will throw exception for non-partitioned
        when(statement.executeQuery(contains("SHOW PARTITIONS")))
                .thenThrow(new SQLException("Table is not partitioned"));

        // Act
        TableMetadata metadata = service.analyzeTable(
                connection, "test_db", "test_table", "updated_at"
        );

        // Assert
        assertNotNull(metadata);
        assertEquals("test_db", metadata.getDatabase());
        assertEquals("test_table", metadata.getTableName());
        assertFalse(metadata.isPartitioned());
        assertEquals(3, metadata.getColumns().size());
        assertTrue(metadata.getColumns().contains("id"));
        assertTrue(metadata.getColumns().contains("name"));
        assertTrue(metadata.getColumns().contains("value"));
        assertFalse(metadata.getColumns().contains("updated_at")); // Excluded
    }

    @Test
    void testAnalyzeTable_Partitioned() throws SQLException {
        // Arrange
        when(connection.createStatement()).thenReturn(statement);

        // Mock SHOW COLUMNS
        when(statement.executeQuery(contains("SHOW COLUMNS"))).thenReturn(resultSet);
        when(resultSet.next())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        when(resultSet.getString(1))
                .thenReturn("id")
                .thenReturn("name");

        // Mock SHOW PARTITIONS - partitioned table
        ResultSet partitionResultSet = mock(ResultSet.class);
        when(statement.executeQuery(contains("SHOW PARTITIONS"))).thenReturn(partitionResultSet);
        when(partitionResultSet.next())
                .thenReturn(true) // First partition exists
                .thenReturn(false);
        when(partitionResultSet.getString(1)).thenReturn("year=2025/month=01");

        // Act
        TableMetadata metadata = service.analyzeTable(
                connection, "test_db", "partitioned_table", ""
        );

        // Assert
        assertNotNull(metadata);
        assertTrue(metadata.isPartitioned());
        assertEquals(2, metadata.getColumns().size());
        assertNotNull(metadata.getPartitionKeys());
        assertEquals("year", metadata.getPartitionKeys().get("1"));
        assertEquals("month", metadata.getPartitionKeys().get("2"));
    }

    @Test
    void testBuildPartitionFilter_NoPartition() {
        // Act
        String filter = service.buildPartitionFilter("NO_PARTITION", "active=1");

        // Assert
        assertEquals("active=1", filter);
    }

    @Test
    void testBuildPartitionFilter_SinglePartition() {
        // Act
        String filter = service.buildPartitionFilter("year=2025", "active=1");

        // Assert
        assertTrue(filter.contains("year='2025'"));
        assertTrue(filter.contains("AND active=1"));
    }

    @Test
    void testBuildPartitionFilter_MultiplePartitions() {
        // Act
        String filter = service.buildPartitionFilter("year=2025/month=01", "active=1");

        // Assert
        assertTrue(filter.contains("year='2025'"));
        assertTrue(filter.contains("month='01'"));
        assertTrue(filter.contains("AND active=1"));
    }

    @Test
    void testGetPartitions() throws SQLException {
        // Arrange
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        when(resultSet.getString(1))
                .thenReturn("year=2025/month=01")
                .thenReturn("year=2025/month=02")
                .thenReturn("year=2025/month=03");

        // Act
        List<String> partitions = service.getPartitions(
                connection, "test_db", "test_table", "1=1"
        );

        // Assert
        assertEquals(3, partitions.size());
        assertEquals("year=2025/month=01", partitions.get(0));
        assertEquals("year=2025/month=02", partitions.get(1));
        assertEquals("year=2025/month=03", partitions.get(2));
    }

    private String contains(String substring) {
        return argThat(sql -> sql != null && sql.contains(substring));
    }
}
