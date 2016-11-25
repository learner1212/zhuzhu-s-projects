package learn.mapreduce.DB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MysqlDB {
    Connection conn = null;
    private String driver = "com.mysql.jdbc.Driver";
    private String URL = "jdbc:mysql://132.122.1.17:3306/odp"; // host1
    private String name = "root";
    private String password = "root123";
    private PreparedStatement pstmt;
    private ResultSet resultSet;

    public void InitConn() throws Exception {
        // final String URL = "jdbc:mysql://10.142.90.63:8808/test"; // host1
        // final String name = "odp";
        // final String password = "odp";
        // 加载驱动程序
        Class.forName(driver);
        // 连接数据库
        conn = DriverManager.getConnection(URL, name, password);
    }

    public MysqlDB(String driver, String URL, String name, String password) {
        this.driver = driver;
        this.URL = URL;
        this.name = name;
        this.password = password;
        try {
            InitConn();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public MysqlDB() {
        try {
            InitConn();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
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
                // if (objects[i] instanceof String) {
                // pstmt.setString(i + 1, (String) objects[i]);
                // } else if (objects[i] instanceof Integer) {
                // pstmt.setInt(i + 1, (Integer) objects[i]);
                // } else if (objects[i] instanceof Long) {
                // pstmt.setLong(i + 1, (Long) objects[i]);
                // }

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
