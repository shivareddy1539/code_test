package com.myproject.dataanalysys.entity;

import java.time.LocalDate;

import lombok.Data;

import javax.persistence.*;

@Entity
@Table(name = "stationdata")
@Data
public class StationData {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
 
    @Column(name = "dateofcapture")
    private LocalDate dataOfCapture;
    
    @Column(name = "cityid")
    private String cityId;
    
    @Column(name = "maxtemp")
    private int maxTemp;
    
    @Column(name = "mintemp")
    private int minTemp;
    
    @Column(name = "precipitation")
    private int precipitation;
}
