package com.myproject.dataanalysys.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.myproject.dataanalysys.entity.StationData;

@Repository
public interface StationDataRepository extends JpaRepository<StationData, Integer>{

	@Query(value = "select * from stationdata where cityid = :station", nativeQuery = true)
	public List<StationData> findAllByStation(@Param(value = "station") String station);

	@Query(value =  "select sum(maxtemp)/count(*)  from stationdata where stationdata.maxtemp != -9999" +
			"  and cityid = :citi and  extract(year from dateofcapture) = :year", nativeQuery = true)
	public Integer getAvgMaxTemp(@Param(value = "citi") String citi, @Param(value = "year") int year);

	@Query(value =  "select sum(mintemp)/count(*)  from stationdata where stationdata.mintemp != -9999" +
			"  and cityid = :citi and  extract(year from dateofcapture) = :year", nativeQuery = true)
	public Integer getAvgMinTemp(@Param(value = "citi") String citi, @Param(value = "year") int year);

	@Query(value =  "select sum(precipitation) from stationdata where cityid = :citi and  extract(year from dateofcapture) = :year and precipitation != -9999", nativeQuery = true)
	public Integer getSumOfPerception(@Param(value = "citi") String citi, @Param(value = "year") int year);

	@Query(value =  "select distinct cityid, extract(year from dateofcapture) from stationdata", nativeQuery = true)
	public List<Object[]> getCityAndYear();
}
