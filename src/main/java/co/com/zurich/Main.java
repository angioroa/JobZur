package co.com.zurich;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import co.com.zurich.job.JobRead;

public class Main {

	public static void main(String[] args) {

		SchedulerFactory schedFact = new StdSchedulerFactory();
		try {

			Scheduler sched = schedFact.getScheduler();
			JobDetail jobRead = JobBuilder.newJob(JobRead.class).withIdentity("jobRead", "group2").build();
			Trigger triggerRead = TriggerBuilder.newTrigger().withIdentity("triggerRead", "group2").startNow()
					.withPriority(15)
					.withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(40).repeatForever())
					.build();

			sched.scheduleJob(jobRead, triggerRead);
			sched.start();

		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}

}
