package com.myproject.dataanalysys.repository;

import com.myproject.dataanalysys.entity.StatisticsData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface StatisticsDataRepository extends JpaRepository<StatisticsData, Integer> {

    @Query(value =  "select * from statistics where city_id = :citi and year = :year", nativeQuery = true)
    StatisticsData findByCityAndYear(@Param(value = "citi") String citi, @Param(value = "year") int year);
}
