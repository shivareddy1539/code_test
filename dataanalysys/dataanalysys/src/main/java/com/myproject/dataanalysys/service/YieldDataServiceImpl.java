package com.myproject.dataanalysys.service;

import com.myproject.dataanalysys.entity.YieldData;
import com.myproject.dataanalysys.repository.YieldDataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class YieldDataServiceImpl implements YieldDataService {

    @Autowired
    private YieldDataRepository yieldDataRepository;

    @Override
    public void saveYieldData(YieldData yieldData) {
        yieldDataRepository.save(yieldData);
    }

    @Override
    public Optional<YieldData> getYieldData(int year) {
        return yieldDataRepository.findById(year);
    }

    @Override
    public List<YieldData> findAll() {
        return yieldDataRepository.findAll();
    }
}
