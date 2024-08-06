package com.enzo;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDDL {

    // 声明一个静态属性
    public static Connection connection = HBaseConnection.connection;

    /**
     * 创建命名空间
     *
     * @param namespace 命名空间名称
     */
    public static void createNamespace(String namespace) throws IOException {
        // 1. 获取admin
        // ❌❌❌ 此处的异常先不要抛出，等待方法全写完，再统一进行处理
        // admin的连接是轻量级的，不是线程安全的，不推荐池化或者缓存这个连接
        Admin admin = connection.getAdmin();


        // 2. 调用方法创建命名空间
        // 代码相对于shell更加底层，所以shell能够实现的功能代码一定能实现
        // 所以需要填写完整的命名空间描述

        // 2.1 创建命名空间描述建造者  =>  设计师
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);

        // 2.2 给命名空间添加需求
        builder.addConfiguration("user", "enzo");

        // 2.3 使用builder构造出对应的添加完参数的对象，完成创建
        // 创建命名空间出现的问题，都属于本方法自身的问题，不应该抛出
        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            System.out.println("命名空间已经存在");
            e.printStackTrace();
        }


        // 3. 关闭admin
        admin.close();
    }


    /**
     * 判断表是否存在
     *
     * @param namespace 命名空间
     * @param tableName 表名
     * @return true 表存在，false 表不存在
     */
    public static boolean isTableExist(String namespace, String tableName) throws IOException {
        // 1. 获取admin
        Admin admin = connection.getAdmin();

        // 2. 调用方法判断表是否存在
        boolean b = false;
        try {
            b = admin.tableExists(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭admin
        admin.close();

        // 4. 返回结果
        return b;
    }

    /**
     * 创建表
     *
     * @param namespace      命名空间
     * @param tableName      表名
     * @param columnFamilies 列族，可以有多个
     */
    public static void createTable(String namespace, String tableName, String... columnFamilies) throws IOException {

        // 0. 判断至少有一个列族
        if (columnFamilies.length == 0) {
            System.out.println("创建表格至少有一个列族");
            return;
        }

        // 0. 判断表格是否存在
        if (isTableExist(namespace, tableName)) {
            System.out.println("表格已经存在");
            return;
        }

        // 1. 获取admin
        Admin admin = connection.getAdmin();

        // 2. 调用方法创建表
        // 2.1 创建表格描述的建造者
        TableDescriptorBuilder tableDescriptorBuilder =
                TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));

        // 2.2 添加参数
        for (String columnFamily : columnFamilies) {
            // 2.3 创建列族描述的建造者
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));

            // 2.4 创建当前的列族添加参数
            // 添加版本参数
            columnFamilyDescriptorBuilder.setMaxVersions(5);


            // 2.5 创建 添加完参数的列族描述
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }

        // 2.6 创建对应的表格描述
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭admin
        admin.close();

    }


    /**
     * 修改表
     *
     * @param namespace    命名空间
     * @param tableName    表名
     * @param columnFamily 列族
     * @param versions     版本
     * @throws IOException
     */
    public static void modifyTable(String namespace, String tableName, String columnFamily, int versions) throws IOException {

        // 0. 判断表格是否存在
        if (!isTableExist(namespace, tableName)) {
            System.out.println("表格不存在");
            return;
        }

        // 1. 获取admin
        Admin admin = connection.getAdmin();
        TableDescriptorBuilder tableDescriptorBuilder =
                null;

        try {
            // 2. 调用方法修改表

            // 2.0 获取之前的表格描述
            TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName));


            // 2.1 创建表格描述的建造者
            // 如果使用填写tableName的方法，相当于创建了一个新的表格描述建造者，没有之前的信息
            // 如果想要之前的信息，必须调用方法填写一个旧的表格描述
            tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);


            // 2.2 创建列族描述的建造者
            ColumnFamilyDescriptor columnFamily1 = descriptor.getColumnFamily(Bytes.toBytes(columnFamily));

            // 需要填写旧的列族描述
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(columnFamily1);

            // 修改对应的版本
            columnFamilyDescriptorBuilder.setMaxVersions(versions);

            // 此处修改的时候，如果填入的是新创建的，那么别的参数会初始化
            tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());

            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }


        // 3. 关闭admin
        admin.close();
    }


    /**
     * 删除表
     *
     * @param namespace 命名空间
     * @param tableName 表名
     * @return true 表格删除成功，false 表格删除失败
     * @throws IOException
     */
    public static boolean deleteTable(String namespace, String tableName) throws IOException {
        // 0. 判断表格是否存在
        if (!isTableExist(namespace, tableName)) {
            System.out.println("表格不存在，无法删除");
            return false;
        }

        // 1. 获取admin
        Admin admin = connection.getAdmin();

        // 2. 调用方法删除表
        try {
            // HBase删除表格之前 一定要先标记表格为不可以写入
            admin.disableTable(TableName.valueOf(namespace, tableName));
            admin.deleteTable(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭admin
        admin.close();

        return true;
    }


    public static void main(String[] args) throws IOException {
        // 测试创建命名空间
        // 应该先保证连接没有问题，再来调用相关的方法
        // createNamespace("enzo");

        // 测试创建表
        // createTable("enzo", "student", "info", "msg");

        // 测试判断表是否存在
        // System.out.println(isTableExist("enzo", "student"));

        // 测试修改表格
        modifyTable("enzo", "student", "info", 6);

        // 其他代码
        System.out.println("其他代码");

        // 关闭连接
        HBaseConnection.closeConnection();
    }
}
