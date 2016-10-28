package sqoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.SqoopTool;
import org.apache.sqoop.util.OptionsFileUtil;

/**
 * Created by jimmy on 2016/10/28.
 * 1，sqoop jar包
 * 2，hadoop-common jar包
 * 3，hadoop-hdfs jar包
 * 4，hadoop-common lib jar包
 * 5，hadoop mapreduce jar包
 * 6，mysql jar包
 *
 * 完成后打jar包，不需要吧依赖的jar包打进去
 *
 * 运行命令：java -Djava.ext.dirs=/opt/cloudera/parcels/CDH/jars:/usr/java/latest/jre/lib/ext/ -jar sqoop1.jar
 *
 * 需求：
 * 1，源数据库可自定义   dmp
 * 2，源表可自定义       dmp_tag
 * 3，源数据库类型可自定义 mysql or oracle
 * 4，hdfs集群地址可自定义
 * 5，输出路径可自定义
 * 6，增量全量自定义、增量字段、增量
 * 7，压缩格式、存储格式自定义
 *
 */

public class importDataFromMysqlToHdfs_v2 {

    public static void main(String[] args)
    {
        try {
            importDataFromMysql();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static int importDataFromMysql() {
        String[] args = new String[]{
                "--connect", "jdbc:mysql://cdh:3306/dmp",
                "--username", "root",
                "--password", "hadoop",
                "--table", "dmp_tag",
/*                "--columns", "LOGIN_NAME,PASSWORD",
                "--split-by", "USER_ID",*/
                "--target-dir", "/user/root/java"
        };

        String[] expandedArgs = null;
        try {
            expandedArgs = OptionsFileUtil.expandArguments(args);
        } catch (Exception  ex) {
            System.err.println(ex.getMessage());
            System.err.println("Try 'sqoop help' for usage.");
        }

        com.cloudera.sqoop.tool.SqoopTool tool = (com.cloudera.sqoop.tool.SqoopTool) SqoopTool.getTool("import");
        //com.cloudera.sqoop.tool.SqoopTool tool = new ImportTool();

        Configuration conf = new Configuration() ;
        conf.set("fs.default.name", "hdfs://cdh:8020/");//设置hadoop服务地址
        Configuration pluginConf = tool.loadPlugins(conf);

        Sqoop sqoop = new Sqoop(tool, pluginConf);
        return Sqoop.runSqoop(sqoop, expandedArgs);
    }

}
