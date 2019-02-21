package com.ctrip.framework.kbear.repository;

import java.util.List;

public interface Repository<Key, Model> {

    Model getRecord(Key id);

    List<Model> getRecords(List<Key> ids);

    List<Model> getAll();

}
