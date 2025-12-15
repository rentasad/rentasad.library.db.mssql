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
    public static final int DEFAULT_MSSQL_PORT = 1433;
    private static final boolean debug = false;
    private static MsSQLConnection instance = null;
    private Connection connection;
    private Map<String, String> connectionParametersMap;

    /**
     * 
     * Description:
     * 
     * @param connectionParametersMap
     * @return
     *         Creation: 15.12.2015 by mst
     */
    public static Connection dbConnect(Map<String, String> connectionParametersMap) throws SQLServerException, ClassNotFoundException
	{
        String msSqlServerUrl = connectionParametersMap.get(MSSQL_DATASOURCE);
        String msSqlDatabaseName = connectionParametersMap.get(MSSQL_DATABASE);
        String msSqlDbUserid = connectionParametersMap.get(MSSQL_USER);
        String msSqlDbPassword = connectionParametersMap.get(MSSQL_PASSWORD);
        return MsSQLConnection.dbConnect(msSqlServerUrl, msSqlDatabaseName, msSqlDbUserid, msSqlDbPassword);
    }

    /**
     * 
     * Description:
     * 
     * @param connectionParametersMap
     * @throws SQLException
     *             Creation: 14.02.2018 by mst
     */
    public static void initInstance(Map<String, String> connectionParametersMap) throws SQLException, ClassNotFoundException
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
    public static Connection dbConnect(String serverUrl, String databaseName, String dbUserid, String dbPassword) throws SQLServerException, ClassNotFoundException
	{
        Connection connection = null;
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

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
    public static Connection dbConnect(String serverUrl, String databaseName) throws ClassNotFoundException, SQLServerException
	{
        Connection connection = null;
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

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
}
