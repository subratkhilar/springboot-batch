package com.javainuse.controller;

import java.util.List;

import javax.batch.operations.JobOperator;
import javax.batch.operations.JobSecurityException;
import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.runtime.BatchRuntime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobInvokerController {
	private static final Logger logger = LoggerFactory.getLogger(JobInvokerController.class);
	@Autowired
	JobLauncher jobLauncher;

	@Autowired
	Job processJob;

	@Autowired
	Job importUserJob;
	@Autowired
	Job readCSVFile;

	@Autowired
	Job sqlToMongo;

	@RequestMapping("/invokejob")
	public String handle() throws Exception {

		JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
				.toJobParameters();
		jobLauncher.run(processJob, jobParameters);

		return "Batch job has been invoked";
	}

	@RequestMapping("/invokeEmpjob")
	public String processEmpData() throws Exception {
		JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
				.toJobParameters();
		jobLauncher.run(importUserJob, jobParameters);
		logger.info("processEmpData ====importUserJob ");
		return "Employee Batch job has been invoked";
	}

	@RequestMapping("/processMongojob")
	public String prosessDataToMongoDB() throws Exception {
		JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
				.toJobParameters();
		jobLauncher.run(readCSVFile, jobParameters);
		return "Data process sucessfully";
	}

	@RequestMapping("/sqlToMongoDBDataProcess")
	public String sqlToMongoDBDataProcess() throws Exception {
		logger.info("sqlToMongoDBDataProcess ====sqlToMongoDBDataProcess ");
		JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
				.toJobParameters();
		jobLauncher.run(sqlToMongo, jobParameters);
		return "Data process sucessfully";
	}

	public void stopBatchJob(boolean flag) throws JobSecurityException, NoSuchJobExecutionException {
		JobOperator jobOperator = BatchRuntime.getJobOperator();
		List<Long> executions = jobOperator.getRunningExecutions("sampleJob");
		jobOperator.stop(executions.iterator().next());

	}
}