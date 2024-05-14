package edu.whu.tmdb.util;

import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
//更改1，防止第一个命令读取时MemManager对外接口还未准备好
import edu.whu.tmdb.query.Transaction;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DbOperation {
    /**
     * 给定元组查询结果，输出查询表格
     * @param result 查询语句的查询结果
     */
    public static void printResult(SelectResult result) {
        // 输出表头信息
        StringBuilder tableHeader = new StringBuilder("|");
        for (int i = 0; i < result.getAttrname().length; i++) {
            tableHeader.append(String.format("%-20s", result.getClassName()[i] + "." + result.getAttrname()[i])).append("|");
        }
        System.out.println(tableHeader);

        // 输出元组信息
        for (Tuple tuple : result.getTpl().tuplelist) {
            StringBuilder data = new StringBuilder("|");
            for (int i = 0; i < tuple.tuple.length; i++) {
                data.append(String.format("%-20s", tuple.tuple[i].toString())).append("|");
            }
            System.out.println(data);
        }
    }

    /**
     * 删除数据库所有数据文件，即重置数据库
     */
    public static void resetDB() {
        // 仓库路径
        //String repositoryPath = "D:\\tmdb";
        String repositoryPath = ".";

        // 子目录路径
        /*String sysPath = repositoryPath + File.separator + "data\\sys";
        String logPath = repositoryPath + File.separator + "data\\log";
        String levelPath = repositoryPath + File.separator + "data\\level";*/
        String sysPath = repositoryPath + File.separator + "data/sys";
        String logPath = repositoryPath + File.separator + "data/log";
        String levelPath = repositoryPath + File.separator + "data/level";

        List<String> filePath = new ArrayList<>();
        filePath.add(sysPath);
        filePath.add(logPath);
        filePath.add(levelPath);

        // 遍历删除文件
        for (String path : filePath) {
            File directory = new File(path);

            // 检查目录是否存在
            if (!directory.exists()) {
                System.out.println("目录不存在：" + path);
                return;
            }

            // 获取目录中的所有文件
            File[] files = directory.listFiles();
            if (files == null) { continue; }
            for (File file : files) {
                // 删除文件
                if (file.delete()) {
                    System.out.println("已删除文件：" + file.getAbsolutePath());
                } else {
                    System.out.println("无法删除文件：" + file.getAbsolutePath());
                }
            }
        }
    }

    public static void showBiPointerTable() {
        // TODO-task2
        Transaction transaction = Transaction.getInstance();
        System.out.println(String.format("|%20s|%20s|%20s|%20s","cls id","obj id","dep cls id","dep obj id"));
        for(BiPointerTableItem item:MemConnect.getBiPointerTableList())
        {
            System.out.println(String.format("|%20d|%20d|%20d|%20d",item.classid,item.objectid,item.deputyid,item.deputyobjectid));
        }
    }

    public static void showClassTable() {
        // TODO-task2
        Transaction transaction = Transaction.getInstance();
        System.out.println(String.format("|%20s|%20s|%20s|%20s|%20s","class name","class id","attribute name","attribute id","attribute type"));
        for (ClassTableItem item : MemConnect.getClassTableList()) {
            System.out.println(String.format("|%20s|%20d|%20s|%20d|%20s",item.classname,item.classid,item.attrname,item.attrid,item.attrtype));
        }
    }

    public static void showDeputyTable() {
        Transaction transaction = Transaction.getInstance();
        System.out.println(String.format("|%20s|%20s","ori cls id","dep cls id"));
        for (DeputyTableItem item : MemConnect.getDeputyTableList()) {
            System.out.println(String.format("|%20d|%20d",item.originid,item.deputyid));
        }
    }

    public static void showSwitchingTable() {
        // TODO-task2
        Transaction transaction = Transaction.getInstance();
        System.out.println(String.format("|%20s|%20s|%20s|%20s|%20s|%20s","origin cls id","origin attr id","origin attr name","dep cls id","dep attr id","dep attr name"));
        for (SwitchingTableItem item : MemConnect.getSwitchingTableList()) {
            System.out.println(String.format("|%20s|%20d|%20s|%20d|%20s|%20s",item.oriId,item.oriAttrid,item.oriAttr,item.deputyId,item.deputyAttrId,item.deputyAttr));
        }
    }
}
