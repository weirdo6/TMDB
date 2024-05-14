package edu.whu.tmdb.query.operations.impl;
//为了方便使用set
import java.util.*;
import edu.whu.tmdb.query.operations.Exception.ErrorList;
import edu.whu.tmdb.storage.memory.MemManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import scala.deprecated;
import scala.annotation.bridge;
//为了便于insert
import edu.whu.tmdb.query.operations.impl.InsertImpl;
import java.io.IOException;
import java.util.stream.Collectors;

import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ClassTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.DeputyTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.SwitchingTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.query.operations.CreateDeputyClass;
import edu.whu.tmdb.query.operations.Insert;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;


public class CreateDeputyClassImpl implements CreateDeputyClass {
    private final MemConnect memConnect;

    public CreateDeputyClassImpl() { this.memConnect = MemConnect.getInstance(MemManager.getInstance()); }

    @Override
    public boolean createDeputyClass(Statement stmt) throws TMDBException, IOException {
        return execute((net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass) stmt);
    }

    public boolean execute(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) throws TMDBException, IOException {
        // 1.获取代理类名、代理类型、select元组
        String deputyClassName = stmt.getDeputyClass().toString();  // 代理类名
        if (memConnect.classExist(deputyClassName)) {
            throw new TMDBException(ErrorList.TABLE_ALREADY_EXISTS, deputyClassName);
        }
        int deputyType = getDeputyType(stmt);   // 代理类型
        Select selectStmt = stmt.getSelect();
        SelectResult selectResult = getSelectResult(selectStmt);

        // 2.执行代理类创建
        return createDeputyClassStreamLine(selectResult, deputyType, deputyClassName);
    }

    public boolean createDeputyClassStreamLine(SelectResult selectResult, int deputyType, String deputyClassName) throws TMDBException, IOException {
        int deputyId = createDeputyClass(deputyClassName, selectResult, deputyType);
        createDeputyTableItem(selectResult.getClassName(), deputyType, deputyId);
        createBiPointerTableItem(selectResult, deputyId);
        return true;
    }

    /**
     * 创建代理类的实现，包含代理类classTableItem的创建和switchingTableItem的创建
     * @param deputyClassName 代理类名称
     * @param selectResult 代理类包含的元组列表
     * @param deputyRule 代理规则
     * @return 新建代理类ID
     */
    private int createDeputyClass(String deputyClassName, SelectResult selectResult, int deputyRule) throws TMDBException {
        // TODO-task3
        //判断名称是否合法
        for (ClassTableItem item : MemConnect.getClassTableList()) {
            if (item.classname.equals(deputyClassName)) {
                throw new TMDBException(ErrorList.TABLE_ALREADY_EXISTS, deputyClassName);
            }
        }
        // 遍历selectResult
        // 1.新建classTableItem
        // 使用MemConnect.getClassTableList().add()
        // 2.新建switchingTableItem
        // 使用MemConnect.getSwitchingTableList().add()
        int length=selectResult.getAttrname().length;
        MemConnect.getClassTable().maxid++;
        int classid = MemConnect.getClassTable().maxid;
        for (int i=0;i<length;i++)
        {
            String store_name="";
            if((selectResult.getAlias())[i]==null||(selectResult.getAlias())[i].equals(""))
            {
                store_name=(selectResult.getAttrname())[i];
            }
            else
            {
                store_name=(selectResult.getAlias())[i];
            }
            memConnect.getClassTableList().add(new ClassTableItem(deputyClassName, classid, length, i,
            store_name, selectResult.getType()[i],"dep",""));  //假定dep为代理类的属性
            memConnect.getSwitchingTableList().add(new SwitchingTableItem(memConnect.getClassId(selectResult.getClassName()[i]),selectResult.getAttrid()[i],selectResult.getAttrname()[i],MemConnect.getClassTable().maxid,
            i,store_name,Integer.toString(deputyRule)));
        }
        return MemConnect.getClassTable().maxid;
    }

    /**
     * 新建deputyTableItem
     * @param classNames 源类类名列表
     * @param deputyType 代理规则
     * @param deputyId 代理类id
     */
    public void createDeputyTableItem(String[] classNames, int deputyType, int deputyId) throws TMDBException {
        // TODO-task3
        // 使用MemConnect.getDeputyTableList().add()
        int length=classNames.length;
        Set<String> class_store_list=new HashSet<>();
        String[] deptype = new String[1];
        deptype[0]=Integer.toString(deputyId);
        for(int i=0;i<length;i++)
        {
            class_store_list.add(classNames[i]);
        }
        for(String item:class_store_list)
        {
            memConnect.getDeputyTableList().add(new DeputyTableItem(memConnect.getClassId(item),deputyId,deptype));
        }
    }

    /**
     * 插入元组，并新建BiPointerTableItem
     * @param selectResult 插入的元组列表
     * @param deputyId 新建代理类id
     */
    private void createBiPointerTableItem(SelectResult selectResult, int deputyId) throws TMDBException, IOException {
        // TODO-task3
        // 使用insert.execute()插入对象
        // 可调用getOriginClass(selectResult);
        // 使用MemConnect.getBiPointerTableList().add()插入BiPointerTable
        InsertImpl insert=new InsertImpl();
        String dep_class_name="";
        for(ClassTableItem item : MemConnect.getClassTableList())
        {
            if(item.classid==deputyId)
            {
                dep_class_name=item.classname;
                break;
            }
        }
        int start_pos=MemConnect.getObjectTable().maxTupleId;
        List<String> attr_strings = new ArrayList<>(Arrays.asList(selectResult.getAttrname()));
        int i=0;
        for(Tuple item:selectResult.getTpl().tuplelist)
        {
            MemConnect.getBiPointerTableList().add(new BiPointerTableItem(item.classId,item.tupleId,deputyId,start_pos+i));
            i++;
        }
        insert.execute(dep_class_name,attr_strings,selectResult.getTpl());
    }

    /**
     * 给定创建代理类语句，返回代理规则
     * @param stmt 创建代理类语句
     * @return 代理规则
     */
    private int getDeputyType(net.sf.jsqlparser.statement.create.deputyclass.CreateDeputyClass stmt) {
        switch (stmt.getType().toLowerCase(Locale.ROOT)) {
            case "selectdeputy":    return 0;
            case "joindeputy":      return 1;
            case "uniondeputy":     return 2;
            case "groupbydeputy":   return 3;
        }
        return -1;
    }

    /**
     * 给定查询语句，返回select查询执行结果（创建deputyclass后面的select语句中的selectResult）
     * @param selectStmt select查询语句
     * @return 查询执行结果（包含所有满足条件元组）
     */
    private SelectResult getSelectResult(Select selectStmt) throws TMDBException, IOException {
        SelectImpl selectExecutor = new SelectImpl();
        return selectExecutor.select(selectStmt);
    }

    private HashSet<Integer> getOriginClass(SelectResult selectResult) {
        ArrayList<String> collect = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(ArrayList::new));
        HashSet<String> collect1 = Arrays.stream(selectResult.getClassName()).collect(Collectors.toCollection(HashSet::new));
        HashSet<Integer> res = new HashSet<>();
        for (String s : collect1) {
            res.add(collect.indexOf(s));
        }
        return res;
    }
}
