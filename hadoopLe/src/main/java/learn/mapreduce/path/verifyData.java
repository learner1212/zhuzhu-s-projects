package learn.mapreduce.path;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import learn.mapreduce.CompareJob;
import learn.mapreduce.Format;
import learn.mapreduce.SampleCompareReduce;
import learn.mapreduce.DB.DataBase;

public class verifyData {
    private String instanceName = "132.122.1.17";
    private String port = "3306";
    private String schemaName = "odp";
    private String tableName;
    private String hiveTableName;
    private String userName = "root";
    private String password = "root123";
    private String targetDir;
    private String hivePath = "hdfs://h3/apps/odp/__hive_db/";
    private double sample_fraction;
    private int compareType;
    private String keyBitSet;
    private String fileNamesStr;
    private String keyStr;
    private String randTableName;
    private Configuration conf;
    private FileSystem fileSystem;
    private FSDataOutputStream fsDataOutputStream;
    private org.apache.hadoop.conf.Configuration hadoopConf;
    private DataBase mysqlDB;
    private DataBase hiveDB;

    private void prepare() {
        try {
            conf = new PropertiesConfiguration("/apps/odp/odp/conf/verifyData.properties");
            hadoopConf = new org.apache.hadoop.conf.Configuration();
        } catch (ConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        mysqlDB = new DataBase("com.mysql.jdbc.Driver", "jdbc:mysql://132.122.1.17:3306/odp", "root", "root123");
        // hiveDB = new DataBase("org.apache.hive.jdbc.HiveDriver",
        // "jdbc:hive2://h3a1.ecloud.com:10000/default;principal=hive/h3a1.ecloud.com@ECLOUD.COM",
        // "hive",
        // "");
        hiveDB = new DataBase("org.apache.hive.jdbc.HiveDriver", "jdbc:hive2://h3a1.ecloud.com:10000/odp;principal=hive/h3a1.ecloud.com@ECLOUD.COM", "hive",
                "");
    }

    public void run(String[] args) {
        tableName = args[0];
        hiveTableName = "odp__" + conf.getString(verifyDataConf.INSTANCENAME).replace(".", "_") + "__" + conf.getString(verifyDataConf.SCHEMANAME) + "__"
                + tableName;
        compareType = Integer.valueOf(args[1]);
        if (compareType == 1) {
            fullCompare();

        } else if (compareType == 2) {
            // sample_fraction = Double.valueOf(args[2]);
            randomCompare();

        } else if (compareType == 3) {

            countCompare();
        } else {
            System.out.println("输入错误");
        }

    }

    public void fullCompare() {
        try {
            String start_date_time = Format.getDateTime();
            checkFileName();
            System.out.println("begin fullCompare");
            instanceName = conf.getString(verifyDataConf.INSTANCENAME);
            port = conf.getString(verifyDataConf.PORT);
            schemaName = conf.getString(verifyDataConf.SCHEMANAME);
            String connectString = "jdbc:mysql://" + instanceName + ":" + port + "/" + schemaName;
            conf.setProperty(verifyDataConf.CONNECTSTRING, connectString);
            ImportToHdfs importToHdfs = new ImportToHdfs(conf);
            int sqoopResult = importToHdfs.run(tableName, keyStr);
            if (sqoopResult != 0) {
                System.out.println("sqoop error!");
                return;
            }
            System.out.println("sqoop end!");
            conf.setProperty(verifyDataConf.HIVEPATH, conf.getString(verifyDataConf.HIVEPATH) + hiveTableName);
            CompareJob fullCompareJob = new CompareJob(conf);
            long[] result = fullCompareJob.runTask();
            fullCompareJob.release();
            if (result[3] == 0) {
                String sql = "INSERT INTO verify_data"
                        + "(instance_name,schema_name,table_name,compare_type,compare_result,source_record_count,dest_record_count,match_record_count,source_match_fraction,dest_match_fraction,compare_start_time,compare_end_time)"
                        + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
                mysqlDB.run(sql, new Object[] { instanceName, schemaName, tableName, compareType, 0, result[0], result[1], result[2], 1.00, 1.00,
                        start_date_time, Format.getDateTime() });

            } else {
                float matchCount = result[2];
                String source_match_fraction = Format.getDecimal(matchCount / result[0]);
                String dest_match_fraction = Format.getDecimal(matchCount / result[0]);
                System.out.println(source_match_fraction + ":" + dest_match_fraction);
                String sql = "INSERT INTO verify_data"
                        + "(instance_name,schema_name,table_name,compare_type,compare_result,source_record_count,dest_record_count,match_record_count,source_match_fraction,dest_match_fraction,compare_start_time,compare_end_time)"
                        + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
                mysqlDB.run(sql, new Object[] { instanceName, schemaName, tableName, compareType, 0, result[0], result[1], result[2], source_match_fraction,
                        dest_match_fraction, start_date_time, Format.getDateTime() });
                int last_insert_id = mysqlDB.getInt("SELECT LAST_INSERT_ID()");
                String result_path = conf.getString(verifyDataConf.RESULTPATH) + conf.getString(verifyDataConf.INSTANCENAME) + "_"
                        + conf.getString(verifyDataConf.SCHEMANAME) + "_" + conf.getString(verifyDataConf.TABLENAME) + "_" + last_insert_id;
                fileSystem = FileSystem.get(hadoopConf);
                fsDataOutputStream = fileSystem.create(new Path(result_path), true);
                fsDataOutputStream.writeBytes("0;" + keyStr + ";" + fileNamesStr + "\n");
                FileStatus[] fileStatus = fileSystem.listStatus(new Path(conf.getString(verifyDataConf.JOBOUPUT)));
                for (FileStatus file : fileStatus) {
                    if (file.getLen() == 0) {
                        continue;
                    }
                    FSDataInputStream fsDataInputStream = fileSystem.open(file.getPath());
                    int ch = 0;
                    while ((ch = fsDataInputStream.read()) != -1) {
                        fsDataOutputStream.write(ch);
                    }
                    fsDataInputStream.close();
                }
                fsDataOutputStream.close();
                fileSystem.close();
                mysqlDB.run("UPDATE verify_data SET diff_file_path=? WHERE id=?", new Object[] { result_path, last_insert_id });

            }

        } catch (InterruptedException e) {
            // TODO: handle exception
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void randomCompare() {
        try {
            checkFileName();
            // int hiveCount = hiveDB.getInt("SELECT COUNT(*) FROM odp." +
            // tableName);
            // int randCount = (int) (hiveCount * sample_fraction);
            // System.out.println(randCount);
            // importToRandTable(randCount);
            String sql = "SELECT table_key_value FROM sample_check";
            ResultSet mysqlRs = mysqlDB.runExec(sql);
            Map<String, List<String>> keyMap = new HashMap<String, List<String>>();
            String[] keys = mysqlDB.getString("SELECT table_key_desc FROM sample_check LIMIT 1").split("\t");
            for (int i = 0; i < keys.length; i++) {
                keyMap.put(keys[i], new ArrayList<String>());
            }
            while (mysqlRs.next()) {
                String[] values = mysqlRs.getString(1).split("\t");
                for (int i = 0; i < values.length; i++) {
                    keyMap.get(keys[i]).add(values[i]);
                }
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < keys.length; i++) {
                sb.append(keys[i]).append(" IN (").append(StringUtils.join(keyMap.get(keys[i]), ",")).append(") AND ");
            }
            sb.delete(sb.length() - 4, sb.length());
            sql = "SELECT * FROM " + tableName + " WHERE " + sb;
            System.out.println("sqoop begin!");
            ImportToHdfs importToHdfs = new ImportToHdfs(conf);
            int sqoopResult = importToHdfs.run(sql, keyStr, '\001');
            if (sqoopResult != 0) {
                System.out.println("sqoop error!");
                return;
            }
            System.out.println("sqoop end!");
            System.out.println("run importHiveToHDFS. please wait..." + Format.getDateTime()); //
            importHiveToHDFS(sql.replaceFirst(tableName, hiveTableName), "hdfs://h3/user/odp/compare/hivepath");
            System.out.println("import to hdfs end!" + Format.getDateTime());
            // sql = "INSERT OVERWRITE DIRECTORY '/user/odp/compare/hivepath' "
            // + "row format delimited " + "fields terminated by '\t' " + sql;
            // System.out.println("run HQL. please wait..." +
            // Format.getDateTime()); //
            // hiveDB.run(sql);
            // System.out.println("import to hdfs end!" + Format.getDateTime());

            conf.setProperty(verifyDataConf.HIVEPATH, "hdfs://h3/user/odp/compare/hivepath");
            CompareJob sampleCompareJob = new CompareJob(conf, SampleCompareReduce.class);
            long[] result = sampleCompareJob.runTask();
            sampleCompareJob.release();
            fileSystem = FileSystem.get(hadoopConf);
            FileStatus[] fileStatus = fileSystem.listStatus(new Path(conf.getString(verifyDataConf.JOBOUPUT)));
            sql = "UPDATE sample_check SET check_result=?,check_result_comment=?,check_time=? WHERE table_key_value=?";
            mysqlDB.batchInit(sql);
            int count = 0;
            System.out.println("write result to mysql,please wait..." + Format.getDateTime());
            for (FileStatus file : fileStatus) {
                if (file.getLen() == 0) {
                    continue;
                }
                FSDataInputStream fsDataInputStream = fileSystem.open(file.getPath());
                String line;
                while ((line = fsDataInputStream.readLine()) != null) {
                    count++;
                    // System.out.println(++count);
                    String[] values = line.split("\003");
                    mysqlDB.addBatch(new Object[] { values[1], values[2].replace('\002', '\n'), values[3], values[0] });
                    if (count % 10000 == 0) {
                        mysqlDB.executeBatch();
                    }
                }
                fsDataInputStream.close();
            }
            mysqlDB.commit();
            System.out.println("write result to mysql end!" + Format.getDateTime());
            fileSystem.close();
        }
        // catch (InterruptedException e) {
        // TODO: handle exception
        // }
        catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void importHiveToHDFS(String sql, String path) {
        try {
            ResultSet hiveRs = hiveDB.runExec(sql);
            int colCount = hiveRs.getMetaData().getColumnCount();
            fileSystem = FileSystem.get(hadoopConf);
            if (fileSystem.exists(new Path(path))) {
                fileSystem.delete(new Path(path), true);
            }
            fsDataOutputStream = fileSystem.create(new Path(path + "/00000"), true);
            while (hiveRs.next()) {
                List<String> row = new ArrayList<String>();
                for (int i = 1; i <= colCount; i++) {
                    row.add(hiveRs.getString(i));
                }
                fsDataOutputStream.writeBytes(StringUtils.join(row, '\001') + '\n');
            }
            fsDataOutputStream.close();
            fileSystem.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void importToRandTable(int randCount) throws SQLException {
        // randTableName = instanceName + "_" + schemaName + "_" + tableName +
        // "_rand";
        String sql = "";
        // if (mysqlDB.isExist(randTableName)) {
        // sql = "UPDATE " + randTableName + " SET
        // table_key=?,table_value=?,insert_time=? WHERE id=1";
        // mysqlDB.run(sql, new Object[] { keyStr, fileNamesStr, getDateTime()
        // });
        // } else {
        // sql = "CREATE TABLE IF NOT EXISTS " + randTableName
        // + "(id INT AUTO_INCREMENT PRIMARY KEY,table_key
        // VARCHAR(200),table_value VARCHAR(2000),insert_time DATETIME)";
        // mysqlDB.run(sql);
        // sql = "INSERT INTO " + randTableName + "
        // (id,table_key,table_value,insert_time)VALUES(?,?,?)";
        // mysqlDB.run(sql, new Object[] { 1, keyStr, fileNamesStr,
        // getDateTime() });
        // }

        String hiveSql = "SELECT * FROM odp." + tableName + " ORDER BY RAND() LIMIT " + randCount;
        ResultSet hiveRs = hiveDB.runExec(hiveSql);
        // int columnCount = hiveRs.getMetaData().getColumnCount();
        sql = "INSERT INTO sample_check (instance_name,schema_name,table_name,table_key_value,table_key_desc,insert_time)VALUES(?,?,?,?,?,?)";
        mysqlDB.batchInit(sql);
        int count = 0;
        while (hiveRs.next()) {
            List<String> keyList = new ArrayList<String>();
            for (int i = 0; i < keyBitSet.length(); i++) {
                if (keyBitSet.charAt(i) == '1') {
                    keyList.add(hiveRs.getString(i + 1));
                }
            }
            System.out.println(++count);
            mysqlDB.addBatch(new Object[] { instanceName, schemaName, tableName, StringUtils.join(keyList, '\001'), keyStr, Format.getDateTime() });
        }
        mysqlDB.commit();
    }

    public void countCompare() {
        try {
            String start_date_time = Format.getDateTime();
            checkFileName();
            int hiveCount = hiveDB.getInt("SELECT COUNT(*) FROM " + hiveTableName);
            int mysqlCount = mysqlDB.getInt("SELECT COUNT(*) FROM " + tableName);
            int compare_result = mysqlCount == hiveCount ? 0 : 1;
            String sql = "INSERT INTO verify_data"
                    + "(instance_name,schema_name,table_name,compare_type,compare_result,source_record_count,dest_record_count,compare_start_time,compare_end_time)"
                    + "VALUES(?,?,?,?,?,?,?,?,?)";
            mysqlDB.run(sql, new Object[] { instanceName, schemaName, tableName, compareType, compare_result, mysqlCount, hiveCount, start_date_time,
                    Format.getDateTime() });
        } catch (InterruptedException e) {
            // TODO: handle exception
        } catch (SQLException e) {
            // TODO: handle exception
        }

    }

    public void checkFileName() throws InterruptedException {
        try {
            ResultSet mysqlRs = mysqlDB.runExec("desc " + tableName);
            // ResultSet hiveRs = hiveDB.runExec("desc
            // odp.stage_xxx_132_122_1_17__odp__" + tableName);
            ResultSet hiveRs = hiveDB.runExec("desc odp." + hiveTableName);
            List<String> mysqlFileNames = new ArrayList<String>();
            List<String> keyFileNames = new ArrayList<String>();
            StringBuilder mysqlKey = new StringBuilder();
            while (mysqlRs.next()) {
                mysqlFileNames.add(mysqlRs.getString(1));
                System.out.println(mysqlRs.getString(1) + ":" + mysqlRs.getString(4));
                if ("PRI".equals(mysqlRs.getString(4))) {
                    keyFileNames.add(mysqlRs.getString(1));
                    mysqlKey.append('1');
                } else {
                    mysqlKey.append('0');
                }
            }
            List<String> hiveFileNames = new ArrayList<String>();
            StringBuilder hiveKey = new StringBuilder();
            while (hiveRs.next()) {
                hiveFileNames.add(hiveRs.getString(1));
                if ("primary key".equals(hiveRs.getString(3))) {
                    hiveKey.append('1');
                } else {
                    hiveKey.append('0');
                }
            }
            if (!mysqlKey.toString().equals(hiveKey.toString()) || !mysqlFileNames.equals(hiveFileNames)) {
                String comment = StringUtils.join(mysqlFileNames, '\001') + "\r\n" + StringUtils.join(hiveFileNames, '\001');
                String dateTime = Format.getDateTime();
                String sql = "INSERT INTO verify_data" + "(instance_name,schema_name,table_name,compare_type,compare_result,compare_start_time,comment)"
                        + "VALUES(?,?,?,?,?,?,?)";
                mysqlDB.run(sql, new Object[] { instanceName, schemaName, tableName, compareType, 2, dateTime, comment });
                throw new InterruptedException();
            }

            keyBitSet = mysqlKey.toString();
            fileNamesStr = StringUtils.join(mysqlFileNames, '\001');
            keyStr = StringUtils.join(keyFileNames, '\001');
            conf.setProperty(verifyDataConf.KEYBITSET, keyBitSet);
            conf.setProperty(verifyDataConf.FILENAME, fileNamesStr);
            conf.setProperty(verifyDataConf.TABLENAME, tableName);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private void release() {
        mysqlDB.close();
        hiveDB.close();
        try {
            if (fsDataOutputStream != null) {
                fsDataOutputStream.close();
            }
            if (fileSystem != null) {
                fileSystem.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            // TODO: handle exception
        }
    }

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        verifyData test = new verifyData();
        test.prepare();
        test.run(args);
        test.release();

    }

}
