/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.clementlevallois.heavydutynlp.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author LEVALLOIS
 */
public class Params {
   /** 
  
* Exemple of paths: note that for windows we need different syntaxes for the path of the cache folder and the path where to save models

* hadoop_home_dir_win=C:\\Users\\levallois\\heavydutynlp\\

* hadoop_home_dir_linux=/home/my/path/models/spark-nlp

* cache_folder_win=file:///c:\\Users\\levallois\\heavydutynlp\\models

* cache_folder_linux=/home/my/path/models/spark-nlp

* path_on_disk_win=c:\\Users\\levallois\\heavydutynlp\\models

* path_on_disk_linux=/home/my/path/models/spark-nlp

*/
    
    public static void main(String args[]) throws IOException {
        System.out.println(Params.getHadoopHomeDir("win"));
    }

    public static String getHadoopHomeDir(String os) throws IOException {
        if (os.equals("win")) {
            return getKey("hadoop_home_dir_win");
        } else {
            return getKey("hadoop_home_dir_linux");
        }
    }

    public static String getCacheFolder(String os) throws IOException {
        if (os.equals("win")) {
            return getKey("cache_folder_win");
        } else {
            return getKey("cache_folder_linux");
        }
    }

    public static String getPathOnDisk(String os) throws IOException {
        if (os.equals("win")) {
            return getKey("path_on_disk_win");
        } else {
            return getKey("path_on_disk_linux");
        }
    }

    private static String getKey(String keyName) throws IOException {

        Properties prop = new Properties();
        String propFileName = "./config.properties";
        FileInputStream file;

        //load the file handle for main.properties
        file = new FileInputStream(propFileName);

        prop.load(file);

        file.close();

        // get the property value and print it out
        return prop.getProperty(keyName);

    }
    
}
