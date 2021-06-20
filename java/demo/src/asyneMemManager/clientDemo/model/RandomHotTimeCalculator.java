package asyneMemManager.clientDemo.model;

import java.util.Random;

import asyncMemManager.common.Configuration;
import asyncMemManager.common.di.HotTimeCalculator;

public class RandomHotTimeCalculator implements HotTimeCalculator{

	@Override
	public void statsWaitingTime(String flowKey, long waitingTime) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long calculate(Configuration config, String flowKey) {
		// TODO Auto-generated method stub
		return (new Random().nextInt() % 5 + 1) * 1000;
	}

}
