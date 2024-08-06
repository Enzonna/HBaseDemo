package com.enzo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConnection {

    // 声明一个静态属性
    public static Connection connection = null;

    static {
        // 1. 创建连接
        // 默认使用同步连接
        try {
            // 使用读取本地文件的形式添加参数
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("获取hbase连接失败");
        }
    }

    public static void closeConnection() throws IOException {
        // 判断连接是否为空
        if (connection != null) {
            // 关闭连接
            connection.close();
        }
    }

    public static void main(String[] args) throws IOException {
        // 直接使用创建好的连接，不要在main线程里面单独创建
        System.out.println(connection);

        // 在main线程的最后记得关闭连接
        closeConnection();
    }
}
