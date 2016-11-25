package learn.mapreduce.path;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.FileLayout;
import com.cloudera.sqoop.tool.ImportTool;

public class ImportToHdfs {
    private Configuration conf;
    private SqoopOptions options;

    public ImportToHdfs(Configuration conf) {
        this.conf = conf;
        prepare();
    }

    private void prepare() {
        options = new SqoopOptions();
        String instanceName = conf.getString(verifyDataConf.INSTANCENAME);
        String port = conf.getString(verifyDataConf.PORT);
        String schemaName = conf.getString(verifyDataConf.SCHEMANAME);
        String connectString = "jdbc:mysql://" + instanceName + ":" + port + "/" + schemaName;
        // options.setDriverClassName(driverClass);
        // options.setDriverClassName(conf.getString(verifyDataConf.DRIVER));
        // options.setConnectString(conf.getString(verifyDataConf.CONNECTSTRING));
        options.setConnectString(connectString);
        options.setUsername(conf.getString(verifyDataConf.USERNAME));
        options.setPassword(conf.getString(verifyDataConf.PASSWORD));
        options.setTargetDir(conf.getString(verifyDataConf.TARGETDIR));
        options.setFileLayout(FileLayout.TextFile);
        options.setNumMappers(1);
        FileSystem fileSystem;
        try {
            fileSystem = FileSystem.get(new org.apache.hadoop.conf.Configuration());
            Path path = new Path(conf.getString(verifyDataConf.TARGETDIR));
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public int run(String tableName, String splitCol) {
        options.setFieldsTerminatedBy('\001');
        String sql = "SELECT * FROM " + tableName + "  WHERE $CONDITIONS";
        options.setSqlQuery(sql);
        options.setSplitByCol(splitCol);
        return new ImportTool().run(options);
    }

    public int run(String sql, String splitCol, char FieldsTerminated) {
        options.setFieldsTerminatedBy(FieldsTerminated);
        if (sql.toUpperCase().contains("WHERE")) {
            options.setSqlQuery(sql + " AND $CONDITIONS");
        } else {
            options.setSqlQuery(sql + " WHERE $CONDITIONS");
        }
        options.setSplitByCol(splitCol);
        return new ImportTool().run(options);
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        // Configuration conf = new Configuration();
        // System.out.println("conf");
        // SqoopOptions options = new SqoopOptions(conf);
        // // ImportTool importTool = new ImportTool();
        // // options.setActiveSqoopTool(importTool);
        // options.setConnectString("jdbc:mysql://192.168.159.128:3306/test");
        // options.setUsername("root");
        // options.setPassword("123456");
        // String sqlQuery = "SELECT * FROM test_checksum_1 WHERE $CONDITIONS";
        // options.setSplitByCol("id");
        // options.setSqlQuery(sqlQuery);
        // options.setTargetDir("hdfs://master:9000/compare/sqlpath");
        // // options.setSplitByCol(taskParam.getSplitByCol());
        // options.setFieldsTerminatedBy('\t');
        // options.setFileLayout(FileLayout.TextFile);
        // options.setNumMappers(1);
        // System.out.println("setting end");
        // // Configuration conf = new Configuration();
        // FileSystem fileSystem = FileSystem.get(conf);
        // Path path = new Path("hdfs://master:9000/compare/sqlpath");
        // if (fileSystem.exists(path)) {
        // fileSystem.delete(path, true);
        // }
        // System.out.println(new ImportTool().run(options));
    }
}
