package com.myproject.dataanalysys.entity;

import lombok.Data;

import javax.persistence.*;

@Entity
@Table(name = "statistics")
@Data
public class StatisticsData {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int id;

    private int year;

    private String cityId;

    private int AvgMaxTemparature;

    private int AvgMinTemparature;

    private int totalPrecipitation;
}