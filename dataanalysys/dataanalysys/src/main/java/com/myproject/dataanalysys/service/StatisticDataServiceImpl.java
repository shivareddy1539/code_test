package com.myproject.dataanalysys.service;

import com.myproject.dataanalysys.entity.StatisticsData;
import com.myproject.dataanalysys.repository.StatisticsDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class StatisticDataServiceImpl implements  StatisticsDataService{

    private static final Logger log = LoggerFactory.getLogger(StatisticDataServiceImpl.class);
    @Autowired
    private StationDataService stationDataService;

    @Autowired
    private StatisticsDataRepository statisticsDataRepository;

    @Override
    public void updateStatistics() {

        List<Object[]> list = stationDataService.getDistinctListAndYear();

        for (int i=0; i< list.size(); i++) {
            Object[] obj = (Object[]) list.get(i);
            String citi = (String) obj[0];
            int year = Integer.valueOf(String.valueOf((BigDecimal) obj[1]));

            log.info("Computing statics for citi " + citi + " year " + year);

            int avgMaxTemparature = stationDataService.getAvgMaxTemp(citi, year);
            int avgMinTemparature = stationDataService.getAvgMinTemp(citi, year);
            int sumOfPerception = stationDataService.getSumOfPerception(citi, year);

            StatisticsData statistics = new StatisticsData();
            statistics.setCityId(citi);
            statistics.setYear(year);
            statistics.setAvgMaxTemparature(avgMaxTemparature);
            statistics.setAvgMinTemparature(avgMinTemparature);
            statistics.setTotalPrecipitation(sumOfPerception);

            StatisticsData statisticsData = statisticsDataRepository.findByCityAndYear(citi, year);
            if (statisticsData != null)
            {
                statistics.setId(statisticsData.getId());
                statisticsDataRepository.save(statistics);
            }
            else
                statisticsDataRepository.save(statistics);
        }
    }

    @Override
    public List<StatisticsData> findAll() {
        return statisticsDataRepository.findAll();
    }
}
