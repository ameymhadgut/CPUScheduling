package SchedulingPolicies;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;

/**
 * 
 * @author arm994@nyu.edu
 * Description: CPU Scheduling Algorithm Simulation for Operating Systems Course at CIMS
 * Date: March 30, 2020 (Unfortunately, the world is dealing with a pandemic COVID-19 during this time)
 *
 */

public class JobScheduling {
	
	/********* START: Wrapper classes for Process Structures ****************************/
	
	/**Class Name: Process 
	* Description: Wrapper class to hold Process details 
	* Attributes:
	* pId: Unique ID for the process
	* arrivalTime: time at which the process arrives
	* cpuBurstTime: CPU burst time for the process
	* */
	class Process {
		long pId;
		long arrivalTime;
		long cpuBurstTime;
		long finishTime;
		long waitTime;
		long turnArndTime;
		boolean executed;
	};
	
	/**Class Name: ProcessDetail 
	* Description: Wrapper class to hold processes & it's details 
	* Attributes: 
	* quantum: used for RR Scheduling
	* noOfProcess: stores total number of Processes
	* processArray: array of Process class objects to store details of individual processes
	* */
	class ProcessDetail {
		long quantum;
		int noOfProcess;
		Process[] processArray = new Process[noOfProcess];
	};
	
	/********* END: Wrapper classes for Process Structures ***************************/
	
	/********* START: Scheduling Algorithm Implementation ****************************/
	
	/**Class Name: Scheduler
	 * Description: Abstract method to implement various job scheduling algorithms
	 * */
	public abstract class Scheduler {
		public abstract void schedule(ProcessDetail p, String inputFileName) throws IOException;
	}
	
	/**Class Name: ShortJobFirst
	 * Description: Contains method to implement Short Job First
	 * */
	public class ShortJobFirst extends Scheduler {
		/**Method: schedule 
		* Description: Accepts the processes, schedules using SJF 
		* 			   and writes to a text file <inputFileName>_SJF
		* */
		public void schedule(ProcessDetail p, String inputFileName) throws IOException {
			p = this.getProcessFinishTime(p);
			p = this.getProcessTAT(p);
			p = this.getProcessWaitTime(p);
			String fileName = inputFileName + "_SJF";
			saveOutputFile(p, fileName);
		}
		
		public ProcessDetail getProcessFinishTime(ProcessDetail p) {
			long sysTime = p.processArray[0].arrivalTime;
			int countOfExeProcess = 0;
			while(true) {
				int curr = p.noOfProcess;
				long min = Long.MAX_VALUE;
				
				if(countOfExeProcess == p.noOfProcess)
				 break;
				
				for(int i = 0; i<p.noOfProcess ; i++) {
					if(p.processArray[i].arrivalTime <= sysTime && !p.processArray[i].executed 
							&& (p.processArray[i].cpuBurstTime < min)) {
						min = p.processArray[i].cpuBurstTime;
						curr = i;
					}
				}
				
				if (curr == p.noOfProcess) {
					sysTime++;
				}
				else {
					p.processArray[curr].finishTime = sysTime + p.processArray[curr].cpuBurstTime;
					sysTime += p.processArray[curr].cpuBurstTime;
					p.processArray[curr].executed = true;
					countOfExeProcess++;
				}
			}
			return p;
		}
		
		public ProcessDetail getProcessTAT(ProcessDetail p) {
			for (int i = 0; i < p.noOfProcess ; i++)  
				p.processArray[i].turnArndTime = p.processArray[i].finishTime - p.processArray[i].arrivalTime;  
		    return p;
		}
		
		public ProcessDetail getProcessWaitTime(ProcessDetail p) {
			for (int i = 0; i < p.noOfProcess ; i++)  
				p.processArray[i].waitTime = p.processArray[i].turnArndTime - p.processArray[i].cpuBurstTime;  
		    return p;
		}
		
	}
	
	/**Class Name: FirstComeFirstServe
	 * Description: Contains method to implement First Come First Serve
	 * */
	public class FirstComeFirstServe extends Scheduler {
		/**Method: schedule 
		* Description: Accepts the processes, schedules using FCFS 
		* 			   and writes to a text file <inputFileName>_FCFS
		* */
		public void schedule(ProcessDetail p, String inputFileName) throws IOException {
			p = this.getProcessFinishTime(p);
			p = this.getProcessTAT(p);
			p = this.getProcessWaitTime(p);
			String fileName = inputFileName + "_FCFS";
			saveOutputFile(p, fileName);
		}
		
		public ProcessDetail getProcessWaitTime(ProcessDetail p) {
			for (int i = 0; i < p.noOfProcess ; i++)  {
				p.processArray[i].waitTime = p.processArray[i].turnArndTime - p.processArray[i].cpuBurstTime;
				if(p.processArray[i].waitTime < 0)
					p.processArray[i].waitTime = 0;
			}
		    return p;
		}
		
		public ProcessDetail getProcessTAT(ProcessDetail p) {
			for (int i = 0; i < p.noOfProcess ; i++)  
				p.processArray[i].turnArndTime = p.processArray[i].finishTime - p.processArray[i].arrivalTime;  
		    return p;
		}
		
		public ProcessDetail getProcessFinishTime(ProcessDetail p) {
			long sysTime = p.processArray[0].arrivalTime; 
		    for (int i = 0; i < p.noOfProcess ; i++)  
		    {  
		    	if(p.processArray[i].arrivalTime <= sysTime) {
			    	sysTime += p.processArray[i].cpuBurstTime;
		    	}
		    	else {
		    		sysTime = p.processArray[i].arrivalTime;
		    		sysTime += p.processArray[i].cpuBurstTime; 
		    	}
		    	p.processArray[i].finishTime = sysTime;
		    }
		    return p;
		}
	}
	
	/**Class Name: RoundRobin
	 * Description: Contains method to implement Round Robin
	 * */
	public class RoundRobin extends Scheduler {
		/**Method: schedule 
		* Description: Accepts the processes, schedules using RR 
		* 			   and writes to a text file <inputFileName>_RR
		* */
		public void schedule(ProcessDetail p, String inputFileName) throws IOException {
			p = this.getProcessFinishTime(p);
			p = this.getProcessTAT(p);
			p = this.getProcessWaitTime(p);
			String fileName = inputFileName + "_RR";
			saveOutputFile(p, fileName);
		}
		
		public ProcessDetail getProcessFinishTime(ProcessDetail p) {
			Queue<Integer> activeQueue = new LinkedList<Integer>();
			Queue<Integer> waitQueue = new LinkedList<Integer>();
			long sysTime = p.processArray[0].arrivalTime;
			long[] remainingTime = new long[p.noOfProcess];
			for(int i = 0; i < p.noOfProcess; i++) {
				remainingTime[i] = p.processArray[i].cpuBurstTime;
			}
			
			for(int i = 0; i < p.noOfProcess; i++) {
				if(p.processArray[i].arrivalTime <= sysTime && !p.processArray[i].executed) {
					waitQueue.add(Integer.valueOf(i));
				}
			}
			
			while(true) {
				activeQueue.addAll(waitQueue);
				waitQueue.clear();
				if(activeQueue.isEmpty() && waitQueue.isEmpty()) {
					int b = 0;
					for(int i = 0; i < p.noOfProcess; i++) {
						if(!p.processArray[i].executed) {
							b = i;
							break;
						}
					}
					sysTime = p.processArray[b].arrivalTime;
					for(int i = 0; i < p.noOfProcess; i++) {
						if(p.processArray[i].arrivalTime <= sysTime && !p.processArray[i].executed) {
							activeQueue.add(i);
						}
					}
				}
				Queue<Integer> activeTemp = new LinkedList<Integer>();
				activeTemp.addAll(activeQueue);
				
				for(int curr: activeQueue) {
					Boolean pending = false;
					if(remainingTime[curr] > p.quantum) {
						remainingTime[curr] -= p.quantum;
						sysTime += p.quantum;
						pending = true;
					}
					else {
						sysTime += remainingTime[curr];
						remainingTime[curr] = 0;
						p.processArray[curr].executed = true;
						p.processArray[curr].finishTime = sysTime;
						waitQueue.remove(curr);
					}
					activeTemp.remove(curr);
					int[] temp = new int[p.noOfProcess];
					int j = 0;
					for(int i = 0; i < p.noOfProcess; i++) {
						if(p.processArray[i].arrivalTime < sysTime && !p.processArray[i].executed 
								&& i != curr && !waitQueue.contains(i) && !activeTemp.contains(i)) {
							waitQueue.add(i);
						}
						else if(p.processArray[i].arrivalTime == sysTime) {
							temp[j] = i;
							j++;
						}
					}
					if(pending)
						waitQueue.add(curr);
					if(j != 0) {
						for(int i = 0; i < j; i++)
							waitQueue.add(temp[i]);
					}
				}
				activeQueue.clear();
				boolean flag = false;
				for(int i = 0; i < p.noOfProcess; i++) {
					if(!p.processArray[i].executed) {
						flag = true;
						break;
					}
				}
				if(!flag)
					break;
			}
			return p;
		}
		
		public ProcessDetail getProcessWaitTime(ProcessDetail p) {
			for (int i = 0; i < p.noOfProcess ; i++)  {
				p.processArray[i].waitTime = p.processArray[i].turnArndTime - p.processArray[i].cpuBurstTime;
				if(p.processArray[i].waitTime < 1) {
					p.processArray[i].waitTime = 0;
				}
			}
		    return p;
		}
		
		public ProcessDetail getProcessTAT(ProcessDetail p) {
			for (int i = 0; i < p.noOfProcess; i++) {
				p.processArray[i].turnArndTime = p.processArray[i].finishTime - p.processArray[i].arrivalTime; 
				if(p.processArray[i].turnArndTime < 1) {
					p.processArray[i].turnArndTime = 0;
				}
			}
		        
			return p;
		}
		
	}
	
	/**Class Name: ShortestRemainingTimeFirst
	 * Description: Contains method to implement Shortest Remaining Time First
	 * */
	public class ShortestRemainingTimeFirst extends Scheduler {
		/**Method: schedule 
		* Description: Accepts the processes, schedules using SRTF 
		* 			   and writes to a text file <inputFileName>_SRTF
		* */
		public void schedule(ProcessDetail p, String inputFileName) throws IOException {
			p = this.getProcessFinishTime(p);
			p = this.getProcessTAT(p);
			p = this.getProcessWaitTime(p);
			String fileName = inputFileName + "_SRTF";
			saveOutputFile(p, fileName);
 		}
		
		public ProcessDetail getProcessFinishTime(ProcessDetail p) {
			long remainingTime[] = new long[p.noOfProcess]; 

	        for (int i = 0; i < p.noOfProcess; i++) 
	        	remainingTime[i] = p.processArray[i].cpuBurstTime; 
	       
	        int countOfExeProcesses = 0; 
	        long sysTime = p.processArray[0].arrivalTime;
	        int shortest = 0; 
	        Boolean newP = false;
	        while (countOfExeProcesses != p.noOfProcess) {
	        	
	        	newP = false;
	            for (int i = 0; i < p.noOfProcess; i++)  
	            { 
	                if ((p.processArray[i].arrivalTime <= sysTime) && !p.processArray[i].executed) {
	                	if(p.processArray[i].pId == 27)
	    	        		System.out.println("1 "+sysTime);
	                	if(newP == false) {
		                    shortest = i;
		                    newP = true;
	                	}
	                	else {
	                		if(remainingTime[i] < remainingTime[shortest]) {
	                			shortest = i;
	                		}
	                	}
	                    
	                } 
	            }
	            if(!newP && countOfExeProcesses != 0) {
	            	if(p.processArray[shortest].pId == 27)
		        		System.out.println("2 "+sysTime);
	            	shortest += 1;
	            	long del = p.processArray[shortest].arrivalTime - sysTime;
	            	sysTime += del;
	            }
	            long diff = sysTime + p.processArray[shortest].cpuBurstTime;
	            int next = 0;
	            Boolean preempt = false;
	            long updatedBurst = 0;
	            long exeTime = 0;
	            long finalExeTime = 0;
	            for (int i = 0; i < p.noOfProcess; i++)  
	            { 
	                if (i!=shortest && (p.processArray[i].arrivalTime <= diff) && !p.processArray[i].executed)
	                {
	                	if(p.processArray[i].pId == 27)
	    	        		System.out.println("3 "+sysTime);
	                	if(p.processArray[i].arrivalTime > sysTime) {
		                	/*if(sysTime <= p.processArray[i].arrivalTime)
		                		exeTime = p.processArray[i].arrivalTime - p.processArray[shortest].arrivalTime;
		                	else*/
		                	exeTime = p.processArray[i].arrivalTime - sysTime;
		                	if(exeTime<0) {
		                		exeTime = 0;
		                	}
		                	updatedBurst = remainingTime[shortest] - exeTime;
		                	Boolean fIn = false;
		                	if(remainingTime[i] < updatedBurst) {
		                		if(!fIn) {
				                    next = i;
				                    preempt = true;
				                    fIn = true;
		                		}
		                		else if(remainingTime[i]<remainingTime[next]) {
		                			next = i;
				                    preempt = true;
		                		}
		                		finalExeTime = exeTime;
		                	}
	                	}
	                }
	            }
	            if(!p.processArray[shortest].executed) {
	            if(!preempt)
	            {
	            	if(p.processArray[shortest].pId == 27)
		        		System.out.println("4 "+sysTime);
	            	sysTime += remainingTime[shortest]; 
	            	p.processArray[shortest].finishTime = sysTime;
	            	p.processArray[shortest].executed = true;
	            	remainingTime[shortest] = 0;
	            	countOfExeProcesses++;
	            }
	            else {
	            	if(p.processArray[shortest].pId == 27)
		        		System.out.println("5 "+sysTime);
	            	sysTime += finalExeTime; 
	            	remainingTime[shortest] -= finalExeTime;
	            }
	            }
	            
	        }
	        return p;
		}
	
		public ProcessDetail getProcessWaitTime(ProcessDetail p) {
			for (int i = 0; i < p.noOfProcess ; i++) {
				p.processArray[i].waitTime = p.processArray[i].turnArndTime - p.processArray[i].cpuBurstTime; 
				if(p.processArray[i].waitTime <= 0) {
					p.processArray[i].waitTime = 0;
				}
			}
		    return p;
		}
		
		public ProcessDetail getProcessTAT(ProcessDetail p) {
			for (int i = 0; i < p.noOfProcess ; i++)  
				p.processArray[i].turnArndTime = p.processArray[i].finishTime - p.processArray[i].arrivalTime;  
		    return p;
		}
	}
	/********* END: Scheduling Algorithm Implementation ****************************/
	
	
	/********* START: Utility Methods **********************************************/
	/**Method: sortProcessByArrival 
	* Description: Accepts the ProcessDetail & sorts it ASC Arrival Time
	* Parameters: 
	* p: Original ProcessDetail
	* Returns: ProcessDetail
	* */
	public ProcessDetail sortProcessByArrival(ProcessDetail p) {
		Process temp = new Process();
		for(int i = 0; i < p.noOfProcess; i++) {
			for(int j = 0; j < (p.noOfProcess - 1); j++) {
				if(p.processArray[j].arrivalTime > p.processArray[j+1].arrivalTime) {
					temp = p.processArray[j];
					p.processArray[j] = p.processArray[j+1];
					p.processArray[j+1] = temp;
				}
				else if(p.processArray[j].arrivalTime == p.processArray[j+1].arrivalTime) {
					if(p.processArray[j].pId > p.processArray[j].pId) {
						temp = p.processArray[j];
						p.processArray[j] = p.processArray[j+1];
						p.processArray[j+1] = temp;
					}
				}
			}
			
		}
		return p;
	}
	
	/**Method: sortProcessByPID 
	* Description: Accepts the ProcessDetail & sorts it ASC PID
	* Parameters: 
	* p: Original ProcessDetail
	* Returns: ProcessDetail
	* */
	public ProcessDetail sortProcessByPID(ProcessDetail p) {
		Process temp = new Process();
		for(int i = 0; i < p.noOfProcess; i++) {
			for(int j = 0; j < (p.noOfProcess - 1); j++) {
				if(p.processArray[j].pId > p.processArray[j+1].pId) {
					temp = p.processArray[j];
					p.processArray[j] = p.processArray[j+1];
					p.processArray[j+1] = temp;
				}
			}
			
		}
		return p;
	}
	
	/**Method: resetProcessDetail 
	* Description: Accepts the ProcessDetail & resets it's attributes
	* Parameters: 
	* p: Original ProcessDetail
	* Returns: ProcessDetail
	* */
	public ProcessDetail resetProcessDetail(ProcessDetail p) {
		for(int i = 0; i < p.noOfProcess; i++) {
			p.processArray[i].waitTime = 0;
			p.processArray[i].turnArndTime = 0;
			p.processArray[i].finishTime = 0;
			p.processArray[i].executed = false;
		}
		return p;
	}
	
	/**Method: saveOutputFile 
	* Description: Accepts the scheduled processes results
	* 			   write & save to a text file 
	* Parameters: 
	* process: Result of the scheduling in the format for each process 
	* 		   <process-id> <finish-time> <wait-time> <turnaround-time>
	* fileName: Input File Name 
	* Returns: void
	* */
	public void saveOutputFile(ProcessDetail pd, String fileName) throws IOException {
		this.sortProcessByPID(pd);
		String[] process = new String[pd.noOfProcess];
		String space = " ";
		//String newLine = "\n";
		for(int i = 0; i < pd.noOfProcess; i++) {
			//if(i == pd.noOfProcess-1)
				//newLine = "";
			process[i] = Long.toString(pd.processArray[i].pId) + space 
						 + Long.toString(pd.processArray[i].finishTime) + space
						 + Long.toString(pd.processArray[i].waitTime) + space
						 + Long.toString(pd.processArray[i].turnArndTime);
		}
		List<String> lines = Arrays.asList(process);
        Files.write(Paths.get(fileName), 
                    lines, 
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, 
                    StandardOpenOption.APPEND);
	}
	
	/**Method: getProcesses 
	* Description: Accepts the filename, read the text file & returns the process 
	* as per the wrapper classes above
	* Parameters: String fileName
	* Returns: ProcessDetail
	* */
	public ProcessDetail getProcesses(String fileName) throws FileNotFoundException {
	    File file = new File(fileName);
	    Scanner sc = new Scanner(file);
	    ProcessDetail pd = new ProcessDetail();
	    int i = 0;
	    while (sc.hasNextLine()) {
	    	if(i == 0)
	    	 pd.quantum = Integer.parseInt(sc.nextLine());
	    	else 
	    	 sc.nextLine();
	    	i++;
	    }
	    sc.close();
	    sc = new Scanner(file);
	    sc.nextLine();
	    pd.noOfProcess = i-1;
	    pd.processArray = new Process[i-1];
	    i = 0;
	    while (sc.hasNextLine()) {
	    	String p = sc.nextLine();
	    	String[] splitP = p.split("\\s+");
	    	pd.processArray[i] = new Process();
	        pd.processArray[i].arrivalTime = Integer.parseInt(splitP[0]);
	        pd.processArray[i].pId = Integer.parseInt(splitP[1]);
	        pd.processArray[i].cpuBurstTime = Integer.parseInt(splitP[2]);
	        i++;
	    }
	    sc.close();
	    return pd;
	}
	
	/********* END: Utility Methods **********************************************/
	
	/********* START: Driver Methods **********************************************/
	
	/**Class Name: AlgorithmName 
	* Description: Constant Class to hold Names of Algos, used by the driver method
	* */
	public final class AlgorithmName {
		public static final String allAlgos = "All";
		public static final String fcfs = "FCFS";
		public static final String sjf = "SJF";
		public static final String rr = "RR";
		public static final String srtf = "SRTF";
	}
	
	/**Method: scheduleProcesses 
	* Description: Accepts the processes & choice of algorithm, saves output file for the result
	* Returns: void
	* */
	public static void scheduleProcesses(ProcessDetail processDetail, JobScheduling js, String algo, String inputFileName) throws IOException {
		if(algo.equals(AlgorithmName.allAlgos)) {
			processDetail = js.resetProcessDetail(processDetail);
		    processDetail = js.sortProcessByArrival(processDetail);
		    Scheduler sc = js.new FirstComeFirstServe();
		    sc.schedule(processDetail, inputFileName);
		    
		    processDetail = js.resetProcessDetail(processDetail);
		    processDetail = js.sortProcessByArrival(processDetail);
		    sc = js.new ShortJobFirst();
		    sc.schedule(processDetail, inputFileName);
		    
		    processDetail = js.resetProcessDetail(processDetail);
		    processDetail = js.sortProcessByArrival(processDetail);
		    sc = js.new RoundRobin();
		    sc.schedule(processDetail, inputFileName);
		    
		    processDetail = js.resetProcessDetail(processDetail);
		    processDetail = js.sortProcessByArrival(processDetail);
		    sc = js.new ShortestRemainingTimeFirst();
		    sc.schedule(processDetail, inputFileName);
		}
		else if(algo.equals(AlgorithmName.fcfs)) {
			processDetail = js.resetProcessDetail(processDetail);
		    processDetail = js.sortProcessByArrival(processDetail);
		    Scheduler sc = js.new FirstComeFirstServe();
		    sc.schedule(processDetail, inputFileName);
		}
		else if(algo.equals(AlgorithmName.sjf)) {
			processDetail = js.resetProcessDetail(processDetail);
		    processDetail = js.sortProcessByArrival(processDetail);
		    Scheduler sc = js.new ShortJobFirst();
		    sc.schedule(processDetail, inputFileName);
		}
		else if(algo.equals(AlgorithmName.rr)) {
			processDetail = js.resetProcessDetail(processDetail);
		    processDetail = js.sortProcessByArrival(processDetail);
		    Scheduler sc = js.new RoundRobin();
		    sc.schedule(processDetail, inputFileName);
		}
		 if(algo.equals(AlgorithmName.srtf)) {
			processDetail = js.resetProcessDetail(processDetail);
		    processDetail = js.sortProcessByArrival(processDetail);
		    Scheduler sc = js.new ShortestRemainingTimeFirst();
		    sc.schedule(processDetail, inputFileName);
		}
	}
	
	/**Method: main 
	 * Description: Driver Method to run Scheduling jobs
	 * */
	public static void main(String[] args) {
		try {
			if (0 < args.length) {
				/* START: Read input file & create a Process Structure */
				String inputFileName = args[0];
				JobScheduling js = new JobScheduling();
			    ProcessDetail processDetail = js.getProcesses(inputFileName);
			    /* END: Read input file & create a Process Structure */
			    
			    /* START: Process Scheduling */
			    scheduleProcesses(processDetail, js, AlgorithmName.allAlgos, inputFileName);
			    /* END: Process Scheduling */
			}
		}
		catch(Exception ex) {
			System.out.println("Exception Occured "+ex.getMessage()+"\nAt line number");
			ex.printStackTrace();
		}
	}
	/********* END: Driver Methods **********************************************/
}
