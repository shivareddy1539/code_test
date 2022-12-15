package com.myproject.dataanalysys.config;

import java.io.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import com.myproject.dataanalysys.entity.StatisticsData;
import com.myproject.dataanalysys.entity.YieldData;
import com.myproject.dataanalysys.service.StationDataService;
import com.myproject.dataanalysys.service.StatisticsDataService;
import com.myproject.dataanalysys.service.YieldDataService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.myproject.dataanalysys.entity.StationData;

@Configuration
public class InsertDataBean {
	
	private static final Logger log = LoggerFactory.getLogger(InsertDataBean.class);

	@Autowired
	private StationDataService stationDataService;

	@Autowired
	private YieldDataService  yieldDataService;

	@Autowired
	private StatisticsDataService statisticsDataService;
	
	@Bean
	public void initiateInsertDataToTables() {
		
		try {
			File folder = ResourceUtils.getFile("classpath:wx_data");
			
			for (final File fileEntry : folder.listFiles()) {
				
				String stationname = fileEntry.getName().replace(".txt", "");
				
				if (stationDataService.getStationDataByCity(stationname).size() > 0)
					continue;				

				log.info("Started data loading for station " + stationname + " time " + LocalDate.now());
				BufferedReader br = new BufferedReader(new FileReader(fileEntry));
				String data =  null; 
				while ((data  = br.readLine()) != null) {
					data = StringUtils.normalizeSpace(data);
					StationData stationData = new StationData();
					String str[] = data.split(" ");

					final LocalDate capturedate = LocalDate.parse(str[0].trim(), DateTimeFormatter.ofPattern("yyyyMMdd"));
					stationData.setDataOfCapture(capturedate);
					stationData.setCityId(stationname);
					stationData.setMinTemp(Integer.valueOf(str[1]));
					stationData.setMaxTemp(Integer.valueOf(str[2]));
					stationData.setPrecipitation(Integer.valueOf(str[3]));

					stationDataService.saveData(stationData);
				}

				br.close();

				log.info("Ended data loading for station " + stationname + " time " + LocalDate.now());
			}
			
		} catch (IOException e) {
			
			log.error("Error while reading", e);
		}

		try {
			File dataFile = ResourceUtils.getFile("classpath:yld_data/US_corn_grain_yield.txt");
			BufferedReader br = new BufferedReader(new FileReader(dataFile));
			String data =  null;
			while ((data  = br.readLine()) != null) {
				data = StringUtils.normalizeSpace(data);
				data = data.replace("\t", " ");
				String str[] = data.split(" ");

				if (yieldDataService.getYieldData(Integer.valueOf(str[0])).isPresent()) continue;

				log.info("Started yield data loading " + data);
				YieldData yieldData = new YieldData();
				yieldData.setYear(Integer.valueOf(str[0]));
				yieldData.setTotalHarvested(Integer.valueOf(str[1]));
				yieldDataService.saveYieldData(yieldData);

				log.info("End yield data loading");
			}

		} catch (Exception e) {
			log.error("Error while reading", e);
		}

	}
}