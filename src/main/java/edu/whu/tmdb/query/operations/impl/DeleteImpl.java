package edu.whu.tmdb.query.operations.impl;


import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.storage.memory.SystemTable.BiPointerTableItem;
import edu.whu.tmdb.storage.memory.SystemTable.ObjectTableItem;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.Delete;
import edu.whu.tmdb.query.operations.Select;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;

public class DeleteImpl implements Delete {

    private MemConnect memConnect;

    public DeleteImpl() {
        this.memConnect = MemConnect.getInstance(MemManager.getInstance());
    }

    @Override
    public void delete(Statement statement) throws JSQLParserException, TMDBException, IOException {
        execute((net.sf.jsqlparser.statement.delete.Delete) statement);
    }

    public void execute(net.sf.jsqlparser.statement.delete.Delete deleteStmt) throws JSQLParserException, TMDBException, IOException {
        // 1.获取符合where条件的所有元组
        Table table = deleteStmt.getTable();        // 获取需要删除的表名
        Expression where = deleteStmt.getWhere();   // 获取delete中的where表达式
        String sql = "select * from " + table;;
        if (where != null) {
            sql += " where " + String.valueOf(where) + ";";
        }
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sql.getBytes());
        net.sf.jsqlparser.statement.select.Select parse = (net.sf.jsqlparser.statement.select.Select) CCJSqlParserUtil.parse(byteArrayInputStream);
        Select select = new SelectImpl();
        SelectResult selectResult = select.select(parse);

        // 2.执行delete
        delete(selectResult.getTpl());
    }

    public void delete(TupleList tupleList) {

        if(tupleList.tuplelist.isEmpty()) return;

        // TODO-task8
        // 1.删除源类tuple和object table

        ArrayList<Integer> deputyTupleIdList = new ArrayList<>();

            // 使用MemConnect.getObjectTableList().remove();   // 删除对象表
            List<ObjectTableItem> removeList = new ArrayList<>();
            List<BiPointerTableItem> removeListBi = new ArrayList<>();
            for(Tuple tuple : tupleList.tuplelist){
                memConnect.DeleteTuple(tuple.tupleId);
                tupleList.tuplenum--;
                for(ObjectTableItem objItem : MemConnect.getObjectTableList()){
                    if(objItem.classid==tuple.classId && objItem.tupleid==tuple.tupleId)
                        removeList.add(objItem);
                }
                for(BiPointerTableItem biItem : MemConnect.getBiPointerTableList()){
                    if(biItem.classid==tuple.classId && biItem.objectid==tuple.tupleId){
                        removeListBi.add(biItem);
                        deputyTupleIdList.add(biItem.deputyobjectid);
                    }
                }
            }
        // 2.删除源类biPointerTable

            // 使用MemConnect.getBiPointerTableList().remove();

        // 3.根据biPointerTable递归删除代理类相关表
        if (deputyTupleIdList.isEmpty()) { return; }
        TupleList deputyTupleList = new TupleList();
        for (Integer deputyTupleId : deputyTupleIdList) {
            Tuple tuple = memConnect.GetTuple(deputyTupleId);
            deputyTupleList.addTuple(tuple);
        }

        MemConnect.getObjectTableList().removeAll(removeList);
        MemConnect.getBiPointerTableList().removeAll(removeListBi);

        delete(deputyTupleList);
    }

}