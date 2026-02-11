package rentasad.library.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import lombok.extern.java.Log;

/**
 * 
 * Gustini GmbH (2015)
 * Creation: 18.03.2015
 * Rentasad Library
 * rentasad.lib.db
 * 
 * @author Matthias Staud
 *
 *         Description:
 *         Klasse zum Herstellen einer Verbindung zu einem MSSQL-Server
 */
@Log
public class MsSQLConnection
{
    public static final String MSSQL_DATASOURCE = "MSSQL_DATASOURCE";
    public static final String MSSQL_USER = "MSSQL_USER";
    public static final String MSSQL_DATABASE = "MSSQL_DATABASE";
    public static final String MSSQL_PASSWORD = "MSSQL_PASSWORD";
    public static final String MSSQL_PORT = "MSSQL_PORT";
    public static final String MSSQL_ENCRYPT = "MSSQL_ENCRYPT";
    public static final String MSSQL_TRUST_SERVER_CERTIFICATE = "MSSQL_TRUST_SERVER_CERTIFICATE";
    public static final String MSSQL_LOGIN_TIMEOUT = "MSSQL_LOGIN_TIMEOUT";
    public static final String MSSQL_CONNECTION_TIMEOUT = "MSSQL_CONNECTION_TIMEOUT";
    public static final String MSSQL_APPLICATION_NAME = "MSSQL_APPLICATION_NAME";
    public static final int DEFAULT_MSSQL_PORT = 1433;
    public static final String DEFAULT_MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final boolean debug = false;
    private static MsSQLConnection instance = null;
    private Connection connection;
    private Map<String, String> connectionParametersMap;

    /**
     * Establishes a connection to the specified SQL Server database using the provided
     * connection parameters from a Map.
     *
     * Supported parameters:
     * - MSSQL_DATASOURCE: Server URL/IP (required)
     * - MSSQL_DATABASE: Database name (required)
     * - MSSQL_USER: Username (required)
     * - MSSQL_PASSWORD: Password (required)
     * - MSSQL_PORT: Port number (optional, default: 1433)
     * - MSSQL_ENCRYPT: Encryption setting (optional, default: "false")
     * - MSSQL_TRUST_SERVER_CERTIFICATE: Trust server certificate (optional, default: "false")
     * - MSSQL_LOGIN_TIMEOUT: Login timeout in seconds (optional)
     * - MSSQL_CONNECTION_TIMEOUT: Connection timeout in seconds (optional)
     * - MSSQL_APPLICATION_NAME: Application name for connection (optional)
     *
     * @param connectionParametersMap Map containing connection parameters
     * @return Connection object representing the established database connection
     * @throws SQLException if connection cannot be established
     *         Creation: 15.12.2015 by mst
     */
    public static Connection dbConnect(Map<String, String> connectionParametersMap) throws SQLException
	{
        String msSqlServerUrl = connectionParametersMap.get(MSSQL_DATASOURCE);
        String msSqlDatabaseName = connectionParametersMap.get(MSSQL_DATABASE);
        String msSqlDbUserid = connectionParametersMap.get(MSSQL_USER);
        String msSqlDbPassword = connectionParametersMap.get(MSSQL_PASSWORD);

        checkDriver();

        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setIntegratedSecurity(false);
        ds.setServerName(msSqlServerUrl);
        ds.setDatabaseName(msSqlDatabaseName);
        ds.setUser(msSqlDbUserid);
        ds.setPassword(msSqlDbPassword);

        // Set port (optional parameter)
        if (connectionParametersMap.containsKey(MSSQL_PORT))
        {
            try
            {
                int port = Integer.parseInt(connectionParametersMap.get(MSSQL_PORT));
                ds.setPortNumber(port);
            } catch (NumberFormatException e)
            {
                log.warning("Invalid port number, using default: " + DEFAULT_MSSQL_PORT);
                ds.setPortNumber(DEFAULT_MSSQL_PORT);
            }
        } else
        {
            ds.setPortNumber(DEFAULT_MSSQL_PORT);
        }

        // Set encryption (optional parameter)
        if (connectionParametersMap.containsKey(MSSQL_ENCRYPT))
        {
            ds.setEncrypt(connectionParametersMap.get(MSSQL_ENCRYPT));
        } else
        {
            ds.setEncrypt("false");
        }

        // Set trust server certificate (optional parameter)
        if (connectionParametersMap.containsKey(MSSQL_TRUST_SERVER_CERTIFICATE))
        {
            ds.setTrustServerCertificate(Boolean.parseBoolean(connectionParametersMap.get(MSSQL_TRUST_SERVER_CERTIFICATE)));
        }

        // Set login timeout (optional parameter)
        if (connectionParametersMap.containsKey(MSSQL_LOGIN_TIMEOUT))
        {
            try
            {
                int timeout = Integer.parseInt(connectionParametersMap.get(MSSQL_LOGIN_TIMEOUT));
                ds.setLoginTimeout(timeout);
            } catch (NumberFormatException e)
            {
                log.warning("Invalid login timeout value: " + connectionParametersMap.get(MSSQL_LOGIN_TIMEOUT));
            }
        }

        // Set connection timeout (optional parameter)
        if (connectionParametersMap.containsKey(MSSQL_CONNECTION_TIMEOUT))
        {
            try
            {
                int timeout = Integer.parseInt(connectionParametersMap.get(MSSQL_CONNECTION_TIMEOUT));
                ds.setSocketTimeout(timeout * 1000); // SQLServerDataSource uses milliseconds
            } catch (NumberFormatException e)
            {
                log.warning("Invalid connection timeout value: " + connectionParametersMap.get(MSSQL_CONNECTION_TIMEOUT));
            }
        }

        // Set application name (optional parameter)
        if (connectionParametersMap.containsKey(MSSQL_APPLICATION_NAME))
        {
            ds.setApplicationName(connectionParametersMap.get(MSSQL_APPLICATION_NAME));
        }

        Connection connection = ds.getConnection();
        System.out.println("connected");
        return connection;
    }

    /**
     * 
     * Description:
     * 
     * @param connectionParametersMap
     * @throws SQLException
     *             Creation: 14.02.2018 by mst
     */
    public static void initInstance(Map<String, String> connectionParametersMap) throws SQLException
	{

        if (instance == null)
        {
            instance = new MsSQLConnection();
            instance.setConnection(MsSQLConnection.dbConnect(connectionParametersMap));
            instance.setConnectionParametersMap(connectionParametersMap);
        } else
        {
            if (debug)
            {
                log.severe("Instance wurde bereits initialisiert");
            }
        }
    }

    /**
     * 
     * Description: GetInstance Class for Managing Connection (and Reconnection after Close and Timeout
     * 
     * @return
     * @throws SQLException
     *             Creation: 14.02.2018 by mst
     */
    public static MsSQLConnection getInstance() throws SQLException
    {
        if (instance == null)
        {
            throw new SQLException("MsSqlConnection Instance not initialized");
        } else
        {
            return instance;
        }

    }

    /**
     * 
     * Description:
     * 
     * @param connectionParametersMap
     *            Creation: 14.02.2018 by mst
     */
    private void setConnectionParametersMap(Map<String, String> connectionParametersMap)
    {
        this.connectionParametersMap = connectionParametersMap;

    }

    private void setConnection(Connection msSqlConnection)
    {
        this.connection = msSqlConnection;
    }

    /**
     * Establishes a connection to the specified SQL Server database using the given
     * credentials and connection parameters.
     *
     * @param serverUrl the URL or IP address of the SQL Server.
     * @param databaseName the name of the database to connect to.
     * @param dbUserid the user ID for the database connection.
     * @param dbPassword the password for the database connection.
     * @return a Connection object representing the established database connection,
     *         or null if the connection cannot be established.
     */
    public static Connection dbConnect(String serverUrl, String databaseName, String dbUserid, String dbPassword) throws SQLException
	{
        Connection connection = null;
        checkDriver();

		SQLServerDataSource ds = new SQLServerDataSource();
            ds.setIntegratedSecurity(false);
            ds.setServerName(serverUrl);
            ds.setPortNumber(DEFAULT_MSSQL_PORT);
            ds.setDatabaseName(databaseName);
            ds.setUser(dbUserid);
            ds.setPassword(dbPassword);
            // disable ssl encryption
            ds.setEncrypt("false");
            connection = ds.getConnection();

            System.out.println("connected");
        return connection;
    }

    /**
     * Establishes a connection to a SQL Server database using Windows authentication.
     *
     * @param serverUrl the URL or IP address of the SQL Server.
     * @param databaseName the name of the database to connect to.
     * @return a Connection object representing the established database connection,
     *         or null if the connection cannot be established.
     */
    public static Connection dbConnect(String serverUrl, String databaseName) throws SQLException
	{
        Connection connection = null;
            checkDriver();

            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setIntegratedSecurity(true);
            ds.setServerName(serverUrl);
            ds.setPortNumber(1433);
            ds.setDatabaseName(databaseName);
            connection = ds.getConnection();

            System.out.println("connected");
        return connection;
    }

    /**
     * @return the connection
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException
	{
        if (this.connection.isValid(4))
        {
            return this.connection;
        } else
        {
            this.connection = MsSQLConnection.dbConnect(this.connectionParametersMap);
            return this.connection;
        }
    }

    public static boolean isInit() throws SQLException
	{
        if (instance != null)
        {
			return instance.getConnection() != null;
        }

        else
            return false;
    }

    /**
     * Verifies the presence of the Microsoft SQL Server JDBC driver in the classpath.
     * If the driver class cannot be found, a {@link SQLException} is thrown.
     *
     * @throws SQLException if the driver class cannot be loaded due to being absent in the classpath.
     */
    private static void checkDriver() throws SQLException
	{
        try
        {
            Class.forName(DEFAULT_MSSQL_DRIVER);
        } catch (ClassNotFoundException e)
        {
            throw new SQLException(e);
        }
    }
}
