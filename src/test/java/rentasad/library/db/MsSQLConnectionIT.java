package rentasad.library.db;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test for MsSQLConnection class.
 *
 * Prerequisites: MSSQL Server container must be running on localhost:1433
 * Start with: docker-compose up -d
 *
 * Connection details:
 * - Host: localhost
 * - Port: 1433
 * - Database: master
 * - User: sa
 * - Password: YourStrong!Passw0rd
 *
 * @author Matthias Staud
 */
class MsSQLConnectionIT
{
    private static String serverUrl;
    private static String databaseName;
    private static String username;
    private static String password;

    @BeforeAll
    static void setUp()
    {
        // Connection details for the already running container
        serverUrl = "localhost";
        databaseName = "master";
        username = "sa";
        password = "YourStrong!Passw0rd";
    }

    @Test
    void testDbConnectWithCredentials() throws SQLException
    {
        // Test the basic connection with credentials
        Connection connection = MsSQLConnection.dbConnect(serverUrl, databaseName, username, password);

        assertNotNull(connection, "Connection should not be null");
        assertFalse(connection.isClosed(), "Connection should be open");
        assertTrue(connection.isValid(5), "Connection should be valid");

        connection.close();
    }

    @Test
    void testDbConnectWithMap() throws Exception
    {
        // Test connection using parameter map
        Map<String, String> connectionParams = new HashMap<>();
        connectionParams.put(MsSQLConnection.MSSQL_DATASOURCE, serverUrl);
        connectionParams.put(MsSQLConnection.MSSQL_DATABASE, databaseName);
        connectionParams.put(MsSQLConnection.MSSQL_USER, username);
        connectionParams.put(MsSQLConnection.MSSQL_PASSWORD, password);

        Connection connection = MsSQLConnection.dbConnect(connectionParams);

        assertNotNull(connection, "Connection should not be null");
        assertFalse(connection.isClosed(), "Connection should be open");
        assertTrue(connection.isValid(5), "Connection should be valid");

        connection.close();
    }

    @Test
    void testExecuteQuery() throws SQLException
    {
        // Test that we can actually execute a query
        Connection connection = MsSQLConnection.dbConnect(serverUrl, databaseName, username, password);

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT 1 AS TestValue");

        assertTrue(resultSet.next(), "ResultSet should have at least one row");
        assertEquals(1, resultSet.getInt("TestValue"), "Query should return 1");

        resultSet.close();
        statement.close();
        connection.close();
    }

    @Test
    void testCreateAndQueryTable() throws SQLException
    {
        // Test creating a table and inserting/querying data
        Connection connection = MsSQLConnection.dbConnect(serverUrl, databaseName, username, password);

        Statement statement = connection.createStatement();

        // Create a test table
        statement.execute("CREATE TABLE TestTable (id INT PRIMARY KEY, name NVARCHAR(50))");

        // Insert test data
        statement.execute("INSERT INTO TestTable (id, name) VALUES (1, 'Test')");

        // Query the data
        ResultSet resultSet = statement.executeQuery("SELECT * FROM TestTable WHERE id = 1");

        assertTrue(resultSet.next(), "ResultSet should have at least one row");
        assertEquals(1, resultSet.getInt("id"), "ID should be 1");
        assertEquals("Test", resultSet.getString("name"), "Name should be 'Test'");

        // Clean up
        statement.execute("DROP TABLE TestTable");

        resultSet.close();
        statement.close();
        connection.close();
    }

    @Test
    void testInstanceInitialization() throws Exception
    {
        // Test singleton instance initialization
        Map<String, String> connectionParams = new HashMap<>();
        connectionParams.put(MsSQLConnection.MSSQL_DATASOURCE, serverUrl);
        connectionParams.put(MsSQLConnection.MSSQL_DATABASE, databaseName);
        connectionParams.put(MsSQLConnection.MSSQL_USER, username);
        connectionParams.put(MsSQLConnection.MSSQL_PASSWORD, password);

        MsSQLConnection.initInstance(connectionParams);

        assertTrue(MsSQLConnection.isInit(), "Instance should be initialized");

        MsSQLConnection instance = MsSQLConnection.getInstance();
        assertNotNull(instance, "Instance should not be null");

        Connection connection = instance.getConnection();
        assertNotNull(connection, "Connection from instance should not be null");
        assertTrue(connection.isValid(5), "Connection from instance should be valid");
    }

    @Test
    void testInvalidConnection()
    {
        // Test connection with invalid credentials
        assertThrows(SQLException.class, () -> {
            MsSQLConnection.dbConnect(serverUrl, databaseName, "invalidUser", "invalidPassword");
        }, "Should throw SQLException for invalid credentials");
    }

    @Test
    void testGetInstanceBeforeInit()
    {
        // This test assumes no other test has initialized the instance
        // In a real scenario, you might need to reset the instance between tests
        assertThrows(SQLException.class, () -> {
            MsSQLConnection.getInstance();
        }, "Should throw SQLException when instance is not initialized");
    }

    @Test
    void testDbConnectWithCustomPort() throws Exception
    {
        // Test connection with custom port parameter
        Map<String, String> connectionParams = new HashMap<>();
        connectionParams.put(MsSQLConnection.MSSQL_DATASOURCE, serverUrl);
        connectionParams.put(MsSQLConnection.MSSQL_DATABASE, databaseName);
        connectionParams.put(MsSQLConnection.MSSQL_USER, username);
        connectionParams.put(MsSQLConnection.MSSQL_PASSWORD, password);
        connectionParams.put(MsSQLConnection.MSSQL_PORT, "1433");

        Connection connection = MsSQLConnection.dbConnect(connectionParams);

        assertNotNull(connection, "Connection should not be null");
        assertTrue(connection.isValid(5), "Connection should be valid");

        connection.close();
    }

    @Test
    void testDbConnectWithEncryptionSettings() throws Exception
    {
        // Test connection with encryption settings
        Map<String, String> connectionParams = new HashMap<>();
        connectionParams.put(MsSQLConnection.MSSQL_DATASOURCE, serverUrl);
        connectionParams.put(MsSQLConnection.MSSQL_DATABASE, databaseName);
        connectionParams.put(MsSQLConnection.MSSQL_USER, username);
        connectionParams.put(MsSQLConnection.MSSQL_PASSWORD, password);
        connectionParams.put(MsSQLConnection.MSSQL_ENCRYPT, "false");
        connectionParams.put(MsSQLConnection.MSSQL_TRUST_SERVER_CERTIFICATE, "true");

        Connection connection = MsSQLConnection.dbConnect(connectionParams);

        assertNotNull(connection, "Connection should not be null");
        assertTrue(connection.isValid(5), "Connection should be valid");

        connection.close();
    }

    @Test
    void testDbConnectWithTimeouts() throws Exception
    {
        // Test connection with timeout parameters
        Map<String, String> connectionParams = new HashMap<>();
        connectionParams.put(MsSQLConnection.MSSQL_DATASOURCE, serverUrl);
        connectionParams.put(MsSQLConnection.MSSQL_DATABASE, databaseName);
        connectionParams.put(MsSQLConnection.MSSQL_USER, username);
        connectionParams.put(MsSQLConnection.MSSQL_PASSWORD, password);
        connectionParams.put(MsSQLConnection.MSSQL_LOGIN_TIMEOUT, "30");
        connectionParams.put(MsSQLConnection.MSSQL_CONNECTION_TIMEOUT, "30");

        Connection connection = MsSQLConnection.dbConnect(connectionParams);

        assertNotNull(connection, "Connection should not be null");
        assertTrue(connection.isValid(5), "Connection should be valid");

        connection.close();
    }

    @Test
    void testDbConnectWithApplicationName() throws Exception
    {
        // Test connection with application name
        Map<String, String> connectionParams = new HashMap<>();
        connectionParams.put(MsSQLConnection.MSSQL_DATASOURCE, serverUrl);
        connectionParams.put(MsSQLConnection.MSSQL_DATABASE, databaseName);
        connectionParams.put(MsSQLConnection.MSSQL_USER, username);
        connectionParams.put(MsSQLConnection.MSSQL_PASSWORD, password);
        connectionParams.put(MsSQLConnection.MSSQL_APPLICATION_NAME, "MsSQLConnectionIT");

        Connection connection = MsSQLConnection.dbConnect(connectionParams);

        assertNotNull(connection, "Connection should not be null");
        assertTrue(connection.isValid(5), "Connection should be valid");

        // Query the application name from the server
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT APP_NAME() AS AppName");

        assertTrue(resultSet.next(), "ResultSet should have at least one row");
        assertEquals("MsSQLConnectionIT", resultSet.getString("AppName"), "Application name should match");

        resultSet.close();
        statement.close();
        connection.close();
    }

    @Test
    void testDbConnectWithAllOptionalParameters() throws Exception
    {
        // Test connection with all optional parameters
        Map<String, String> connectionParams = new HashMap<>();
        connectionParams.put(MsSQLConnection.MSSQL_DATASOURCE, serverUrl);
        connectionParams.put(MsSQLConnection.MSSQL_DATABASE, databaseName);
        connectionParams.put(MsSQLConnection.MSSQL_USER, username);
        connectionParams.put(MsSQLConnection.MSSQL_PASSWORD, password);
        connectionParams.put(MsSQLConnection.MSSQL_PORT, "1433");
        connectionParams.put(MsSQLConnection.MSSQL_ENCRYPT, "false");
        connectionParams.put(MsSQLConnection.MSSQL_TRUST_SERVER_CERTIFICATE, "true");
        connectionParams.put(MsSQLConnection.MSSQL_LOGIN_TIMEOUT, "30");
        connectionParams.put(MsSQLConnection.MSSQL_CONNECTION_TIMEOUT, "30");
        connectionParams.put(MsSQLConnection.MSSQL_APPLICATION_NAME, "FullParamTest");

        Connection connection = MsSQLConnection.dbConnect(connectionParams);

        assertNotNull(connection, "Connection should not be null");
        assertTrue(connection.isValid(5), "Connection should be valid");

        connection.close();
    }
}
