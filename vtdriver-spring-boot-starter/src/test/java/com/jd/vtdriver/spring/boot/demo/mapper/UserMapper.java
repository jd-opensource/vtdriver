package com.jd.vtdriver.spring.boot.demo.mapper;

import com.jd.vtdriver.spring.boot.demo.model.User;
import java.util.List;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

public interface UserMapper {

    @Select("select * from user")
    @Results({
        @Result(property = "id", column = "id"),
        @Result(property = "name", column = "name")
    })
    List<User> getAll();

}
