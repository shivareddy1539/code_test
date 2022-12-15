package com.myproject.dataanalysys.entity;

import lombok.Data;

import javax.persistence.*;

@Entity
@Table(name = "yielddata")
@Data
public class YieldData {
	
    @Id
	private int year;
	
	@Column(name = "totalharvested")
	private int totalHarvested;
}
