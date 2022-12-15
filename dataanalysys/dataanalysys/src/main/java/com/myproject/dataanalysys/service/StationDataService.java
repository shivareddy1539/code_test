package com.myproject.dataanalysys.service;

import com.myproject.dataanalysys.entity.StationData;

import java.util.List;

//  Service to class to get station data
public interface StationDataService {

    public void saveData(StationData stationData);

    List<StationData> getStationDataByCity(String stationName);

    List<Object[]> getDistinctListAndYear();

    int getAvgMaxTemp(String citi, int year);

    int getAvgMinTemp(String citi, int year);

    int getSumOfPerception(String citi, int year);

    List<StationData> findAll();
}