package com.myproject.dataanalysys.service;

import com.myproject.dataanalysys.entity.StationData;
import com.myproject.dataanalysys.repository.StationDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class StationDataServiceImpl implements StationDataService{
    private static final Logger log = LoggerFactory.getLogger(StationDataServiceImpl.class);

    @Autowired
    private StationDataRepository stationDataRepository;

    @Override
    public void saveData(StationData stationData) {
        try {
            stationDataRepository.save(stationData);
        }
        catch (Exception exception) {
            log.error("Exception while saving {}", exception);
        }
    }

    @Override
    public List<StationData> getStationDataByCity(String stationName) {
        return stationDataRepository.findAllByStation(stationName);
    }

    @Override
    public List<Object[]> getDistinctListAndYear() {
        return  stationDataRepository.getCityAndYear();
    }

    @Override
    public int getAvgMaxTemp(String citi, int year) {
        Integer value = stationDataRepository.getAvgMaxTemp(citi, year);
        return value == null ? 0 : value;
    }

    @Override
    public int getAvgMinTemp(String citi, int year) {
        Integer value = stationDataRepository.getAvgMinTemp(citi, year);
        return value == null ? 0 : value;
    }

    @Override
    public int getSumOfPerception(String citi, int year) {
        Integer value = stationDataRepository.getSumOfPerception(citi, year);
        return value == null ? 0 : value;
    }

    @Override
    public List<StationData> findAll() {
        return stationDataRepository.findAll();
    }
}

