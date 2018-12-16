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
			BizQuestOne.run(args);
		case "2":
		case "3":
		case "4":
		case "5":
			System.out.println("not impleneted yet");
		}
		
	}
}
