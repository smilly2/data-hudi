package cn.com.kltb.data.hudi;

import org.apache.hadoop.conf.Configuration;

/**
 * Create by jiangxf@kltb.com.cn at 2022/6/8
 */
public class Config {
    static Configuration hadoopConf = new Configuration();
    static {
        //oss 配置
        hadoopConf.set("fs.defaultFS","oss://bucket Name/");
        hadoopConf.set("fs.oss.endpoint","oss-cn-shenzhen.aliyuncs.com");
        hadoopConf.set("fs.oss.impl","org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
        hadoopConf.set("fs.oss.accessKeyId","");
        hadoopConf.set("fs.oss.accessKeySecret","");
    }
}
