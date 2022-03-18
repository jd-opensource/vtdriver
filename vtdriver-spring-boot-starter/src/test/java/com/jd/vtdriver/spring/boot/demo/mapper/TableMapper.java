package com.jd.vtdriver.spring.boot.demo.mapper;

import com.jd.vtdriver.spring.boot.demo.model.Table;
import java.util.List;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

public interface TableMapper {

    @Select("select * from table_engine_test limit 10")
    @Results({
        @Result(property = "id", column = "id"),
        @Result(property = "fKey", column = "f_key")
    })
    List<Table> getTen();
}
