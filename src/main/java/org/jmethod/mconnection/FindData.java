package org.jmethod.mconnection;

import java.util.ArrayList;
import java.util.List;

public class FindData {
    private List<DbData> dbDatas = new ArrayList<>();
    private String exStr = null;

    // create
    public static FindData createFindData(List<DbData> dbDatas){
        FindData fd = new FindData();
        fd.dbDatas = dbDatas;
        return fd;
    } // createFindData

    public void setDbDatas(List<DbData> dbDatas) {
        this.dbDatas = dbDatas;
    }
    public List<DbData> getDbDatas() {
        return dbDatas;
    }

    public String getExStr() {
        return exStr;
    }
    public void setExStr(String exStr) {
        this.exStr = exStr;
    }

    public List<Object> toIds(){
        List<Object> list = new ArrayList<>();
        if (dbDatas.isEmpty()) {
            return list;
        }

        for (int i = 0; i < dbDatas.size(); i++) {
            list.add(dbDatas.get(i).getId());
        }
        return list;
    }

    public int getQuant(){
        return dbDatas.size();
    }

    public Object getFirstId(){
        if (dbDatas.isEmpty()) {
            return null;
        } else {
            return dbDatas.get(0).getId();
        }
    }

    public DbData getFirstDbr(){
        if (dbDatas.isEmpty()) {
            return null;
        } else {
            return dbDatas.get(0);
        }
    }
}
