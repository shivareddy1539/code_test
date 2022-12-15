package com.myproject.dataanalysys.service;

import com.myproject.dataanalysys.entity.YieldData;

import java.util.List;
import java.util.Optional;

public interface YieldDataService {
    void saveYieldData(YieldData yieldData);

    Optional<YieldData> getYieldData(int year);

    List<YieldData> findAll();
}
