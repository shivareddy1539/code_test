package com.myproject.dataanalysys.service;

import com.myproject.dataanalysys.entity.StatisticsData;

import java.util.List;

public interface StatisticsDataService {
    void updateStatistics();

    List<StatisticsData> findAll();
}
