package com.enzo;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDML {
    // 静态属性
    public static Connection connection = HBaseConnection.connection;


    /**
     * 插入数据
     *
     * @param namespace    命名空间
     * @param tableName    表名
     * @param rowKey       主键
     * @param columnFamily 列族
     * @param columnName   列名
     * @param value        值
     */
    public static void putCell(String namespace, String tableName, String rowKey, String columnFamily, String columnName, String value) throws IOException {

        // 1. 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 调用相关方法插入数据
        // 2.1 创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        // 2.2 给put对象添加属性
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));

        // 2.3 将对象写入对应的方法
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 3. 关闭table
        table.close();
    }


    /**
     * 查询对应一行中的某一列数据
     *
     * @param namespace    命名空间
     * @param tableName    表名
     * @param rowKey       主键
     * @param columnFamily 列族
     * @param columnName   列名
     * @throws IOException
     */
    public static void getCells(String namespace, String tableName, String rowKey, String columnFamily, String columnName) throws IOException {
        // 1. 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        // 如果直接调用get方法，则默认查询一整行数据
        // 如果想读取某一列的数据，则需要添加对应的参数
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        // 设置读取数据的版本
        get.readAllVersions();

        try {
            // 读取数据，得到result对象
            Result result = table.get(get);

            // 处理数据
            Cell[] cells = result.rawCells();

            // 测试方法：直接把读取的数据打印到控制台
            // 如果是实际开发，需要在额外写方法对应处理数据
            for (Cell cell : cells) {
                String value = new String(CellUtil.cloneValue(cell));
                System.out.println(value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        // 3. 关闭table
        table.close();
    }


    /**
     * 扫描数据
     *
     * @param namespace   命名空间
     * @param tableName   表名
     * @param startRowKey 开始的row，包含
     * @param stopRowKey  结束的row，不包含
     * @throws IOException
     */
    public static void scanRows(String namespace, String tableName, String startRowKey, String stopRowKey) throws IOException {
        // 1. 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 创建scan对象
        Scan scan = new Scan();
        // 如果此时直接调用，会直接扫描整张表

        // 添加参数，来控制扫描的数据
        scan.withStartRow(Bytes.toBytes(startRowKey));  // 默认包含
        scan.withStopRow(Bytes.toBytes(stopRowKey));    // 默认不包含

        try {
            // 读取多行数据，获得scanner
            ResultScanner scanner = table.getScanner(scan);

            // result来记录一行数据        cell数组
            // ResultScanner来记录多行数据 result数据
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.print(new String(CellUtil.cloneRow(cell)) + "-" + new String(CellUtil.cloneFamily(cell)) +
                            "-" + new String(CellUtil.cloneQualifier(cell)) + "-" + new String(CellUtil.cloneValue(cell)) + "\t");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭table
        table.close();

    }


    /**
     * 过滤扫描数据
     *
     * @param namespace    命名空间
     * @param tableName    表名
     * @param startRowKey  开始的row，包含
     * @param stopRowKey   结束的row，不包含
     * @param columnFamily 列族
     * @param columnName   列名
     * @param value        值
     * @throws IOException
     */
    public static void filterScan(String namespace, String tableName, String startRowKey, String stopRowKey, String columnFamily, String columnName, String value) throws IOException {
        // 1. 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 创建scan对象
        Scan scan = new Scan();
        // 如果此时直接调用，会直接扫描整张表

        // 添加参数，来控制扫描的数据
        scan.withStartRow(Bytes.toBytes(startRowKey));  // 默认包含
        scan.withStopRow(Bytes.toBytes(stopRowKey));    // 默认不包含

        // 可以添加多个过滤
        FilterList filterList = new FilterList();

        // 创建过滤器
        // (1) 结果只保留当前列的数据
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                // 列族
                Bytes.toBytes(columnFamily),
                // 列名
                Bytes.toBytes(columnName),
                // 比较关系
                CompareOperator.EQUAL,
                // 值
                Bytes.toBytes(value)
        );

        // (2) 结果保留整行数据
        // 结果同时会保留没有当前列的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                // 列族
                Bytes.toBytes(columnFamily),
                // 列名
                Bytes.toBytes(columnName),
                // 比较关系
                CompareOperator.EQUAL,
                // 值
                Bytes.toBytes(value)
        );


        // 本身可以添加多个过滤器
        filterList.addFilter(singleColumnValueFilter);


        // 添加过滤
        scan.setFilter(filterList);


        try {
            // 读取多行数据，获得scanner
            ResultScanner scanner = table.getScanner(scan);

            // result来记录一行数据        cell数组
            // ResultScanner来记录多行数据 result数据
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.print(new String(CellUtil.cloneRow(cell)) + "-" + new String(CellUtil.cloneFamily(cell)) +
                            "-" + new String(CellUtil.cloneQualifier(cell)) + "-" + new String(CellUtil.cloneValue(cell)) + "\t");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭table
        table.close();

    }


    /**
     * 删除一行中的一列数据
     *
     * @param namespace    命名空间
     * @param tableName    表名
     * @param rowKey       主键
     * @param columnFamily 列族
     * @param columnName   列名
     * @throws IOException
     */
    public static void deleteCell(String namespace, String tableName, String rowKey, String columnFamily, String columnName) throws IOException {
        // 1. 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 创建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // 添加列信息
        // addColumn删除一个版本
        // delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        // addColumns删除所有版本
        // 按照逻辑需要删除所有版本的数据
        delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭table
        table.close();
    }


    public static void main(String[] args) throws IOException {

        // 测试添加列族
        // putCell("enzo", "student", "1003", "info", "name", "chenli");
        // putCell("enzo", "student", "1003", "msg", "name", "my love");


        // 测试读取数据
        // getCells("enzo", "student", "1001", "info", "name");

        // 测试扫描数据
        // scanRows("enzo", "student", "1001", "1004");

        // 带过滤扫描数据
        filterScan("enzo", "student", "1001", "1004", "info", "name", "chenli");


        // 其他代码
        System.out.println("其他代码");

        // 关闭连接
        HBaseConnection.closeConnection();
    }
}
