package com.ctrip.framework.kbear.repository;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

@RunWith(Parameterized.class)
public abstract class RepositoryTest<Key, Model> {

    private Repository<Key, Model> _repository;

    @Parameter(0)
    public Key _id;

    @Parameter(1)
    public Model _record;

    @Parameter(2)
    public List<Key> _ids;

    @Parameter(3)
    public List<Model> _records;

    @Parameter(4)
    public List<Model> _all;

    public RepositoryTest() {
        _repository = newRepository();
    }

    @Test
    public void getRecordTest() {
        Model record = _repository.getRecord(_id);
        Assert.assertEquals(_record, record);
    }

    @Test
    public void getRecordsTest() {
        List<Model> records = _repository.getRecords(_ids);
        Assert.assertEquals(_records, records);
    }

    @Test
    public void getAllTest() {
        List<Model> all = _repository.getAll();
        Assert.assertEquals(_all, all);
    }

    protected abstract Repository<Key, Model> newRepository();

    protected Repository<Key, Model> getRepository() {
        return _repository;
    }

}
