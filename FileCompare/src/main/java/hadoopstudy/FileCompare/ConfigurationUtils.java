package hadoopstudy.FileCompare;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;

public class ConfigurationUtils {
	
	public static Configuration getConf(String path) throws FileNotFoundException{
		File file = new File(path);
		if (!file.exists()) {
			throw new FileNotFoundException();
		}
		Configuration configuration = new Configuration();
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = null;
		try {
			while ((line = bufferedReader.readLine()) != null) {
				if (line.startsWith("#")) {
					continue;
				}
				String[]temp = line.split("=");
				if (temp.length != 2) {
					System.out.println("文件内容格式出错");
					continue;
				}
				configuration.set(temp[0].trim(), temp[1].trim());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return configuration;
	}
	
	public static org.apache.commons.configuration.Configuration getConf1(String path) throws FileNotFoundException{
		File file = new File(path);
		if (!file.exists()) {
			throw new FileNotFoundException();
		}
		org.apache.commons.configuration.Configuration configuration = new PropertiesConfiguration();
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = null;
		try {
			while ((line = bufferedReader.readLine()) != null) {
				if (line.startsWith("#")) {
					continue;
				}
				String[]temp = line.split("=");
				if (temp.length != 2) {
					System.out.println("文件内容格式出错");
					continue;
				}
				configuration.setProperty(temp[0].trim(), temp[1].trim());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return configuration;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Configuration configuration = getConf("E:\\workspace\\zhuzhu-s-projects\\FileCompare\\test.properies");
			Iterator<Entry<String, String>> iterator = configuration.iterator();
			while (iterator.hasNext()) {
				Entry<String, String> entry = iterator.next();
				System.out.println(entry.getKey()+"   :   "+entry.getValue());
			}
//			org.apache.commons.configuration.Configuration configuration = getConf1("test.properies");
//			Iterator<String> iterator = configuration.getKeys();
//			while (iterator.hasNext()) {
//				String key = (String) iterator.next();
//				System.out.println(key + "    :     " + configuration.getString(key));
//			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
