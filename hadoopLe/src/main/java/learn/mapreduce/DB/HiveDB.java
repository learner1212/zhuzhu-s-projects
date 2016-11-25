package learn.mapreduce.DB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveDB {
    private String url = "jdbc:hive2://h3a1.ecloud.com:10000/default;principal=hive/h3a1.ecloud.com@ECLOUD.COM";
    private String driverName = "org.apache.hive.jdbc.HiveDriver";
    private String defaultUrl = "";
    private PreparedStatement pstmt;
    private Connection conn;
    private ResultSet resultSet;

    // public JdbcExecHive(HiveConf hiveConf) {
    // this.url = hiveConf.get("odp.hive.jdbc.url", defaultUrl);
    // }

    public HiveDB() {
        try {
            init();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void init() throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url, "hive", "");
    }

    public int run(String sql) throws SQLException {
        return run(sql, null);
    }

    public int run(String sql, Object[] objects) throws SQLException {
        pstmt = conn.prepareStatement(sql);
        if (objects != null) {
            for (int i = 0; i < objects.length; i++) {
                pstmt.setString(i + 1, String.valueOf(objects[i]));
            }
        }
        return pstmt.executeUpdate();
    }

    public ResultSet runExec(String sql) throws SQLException {
        return runExec(sql, null);
    }

    public ResultSet runExec(String sql, Object[] objects) throws SQLException {
        pstmt = conn.prepareStatement(sql);
        if (objects != null) {
            for (int i = 0; i < objects.length; i++) {
                pstmt.setString(i + 1, String.valueOf(objects[i]));
            }
        }
        resultSet = pstmt.executeQuery();
        return resultSet;
    }

    public int getInt(String sql) throws SQLException {
        return getInt(sql, null);
    }

    public int getInt(String sql, Object[] objects) throws SQLException {
        int result = 0;
        pstmt = conn.prepareStatement(sql);
        if (objects != null) {
            for (int i = 0; i < objects.length; i++) {
                pstmt.setString(i + 1, String.valueOf(objects[i]));
            }
        }
        resultSet = pstmt.executeQuery();
        while (resultSet.next()) {
            result = resultSet.getInt(1);
        }
        return result;
    }

    public void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (pstmt != null) {
                pstmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            // TODO: handle exception
        }

    }
}
