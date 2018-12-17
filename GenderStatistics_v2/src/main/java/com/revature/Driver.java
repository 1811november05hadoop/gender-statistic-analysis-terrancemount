package com.revature;

import java.io.IOException;

public class Driver {
	public static void main(String[] args) 
			throws ClassNotFoundException, IOException, InterruptedException {
		
		if(args.length < 3){
			System.err.println("Usage: Driver"
					+ "<Input filename> <Output filename> "
					+ "<BizQuestNumber>");
			System.exit(-1);
		}
		
		switch(args[2]){
		case "1":
			com.revature.BizQuestOne.BizDriver.run(args);
			break;
		case "2":
			com.revature.BizQuestTwo.BizDriver.run(args);
			break;
		case "3":
			com.revature.BizQuestThree.BizDriver.run(args);
			break;
		case "4":
			com.revature.BizQuestFour.BizDriver.run(args);
			break;
		case "5":
			com.revature.BizQuestFive.BizDriver.run(args);
		}
		
	}
}
