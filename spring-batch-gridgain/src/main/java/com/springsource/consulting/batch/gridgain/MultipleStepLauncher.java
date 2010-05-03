package com.springsource.consulting.batch.gridgain;

import org.gridgain.grid.GridFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class MultipleStepLauncher {

	static {
		System.setProperty("GRIDGAIN_HOME", "c:/Programs/gridgain-2.0.3");
	}

	public static void main(String[] args) throws Exception {

		GridFactory.start();

		try {
			ApplicationContext context = new ClassPathXmlApplicationContext(
					"/launch-context.xml");

			Job job = (Job) context.getBean("job1");
			JobLauncher jobLauncher = (JobLauncher) context
					.getBean("jobLauncher");

			JobExecution jobExecution;
			try {
				jobExecution = jobLauncher.run(job, new JobParametersBuilder()
						.addLong("timestamp", System.currentTimeMillis())
						.toJobParameters());
			} catch (JobExecutionAlreadyRunningException e) {
				throw new IllegalStateException(e);
			} catch (JobRestartException e) {
				throw new IllegalStateException(e);
			} catch (JobInstanceAlreadyCompleteException e) {
				throw new IllegalStateException(e);
			}

			System.out.println("Result is " + jobExecution);

		} finally {
			GridFactory.stop(true);
		}

	}

}