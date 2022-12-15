package com.myproject.dataanalysys.controller;

import com.myproject.dataanalysys.entity.StationData;
import com.myproject.dataanalysys.entity.StatisticsData;
import com.myproject.dataanalysys.entity.YieldData;
import com.myproject.dataanalysys.service.StationDataService;
import com.myproject.dataanalysys.service.StatisticsDataService;
import com.myproject.dataanalysys.service.YieldDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class AnalysisController {

    @Autowired
    private StationDataService stationDataService;

    @Autowired
    private StatisticsDataService statisticsDataService;

    @Autowired
    private YieldDataService yieldDataService;

    /**
     * gets all the statics data
     * @return
     */
    @GetMapping("/api/weather/stats")
    public List<StatisticsData> getStatisTics() {
        return statisticsDataService.findAll();
    }

    /**
     * get all the Yield data
     * @return
     */
    @GetMapping("/api/yield")
    public List<YieldData> getYieldDataList() {
        return yieldDataService.findAll();
    }

    /**
     * get all station data
     * @return
     */
    @GetMapping("/api/weather")
    public List<StationData> getStationDataList() {
        return stationDataService.findAll();
    }

    @PostMapping("/updatestatistics")
    public ResponseEntity updateStatistics() {
        statisticsDataService.updateStatistics();
        return ResponseEntity.ok("Success");
    }
}
