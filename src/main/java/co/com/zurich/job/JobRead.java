package co.com.zurich.job;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import co.com.zurich.controller.ReadFile;

public class JobRead implements Job {

	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		System.out.println("This is the job A entrada");
		Logger logger = Logger.getLogger("MyLog");
		FileHandler fh;

		try {

			fh = new FileHandler("C:\\rutaLog\\archivo.log", true);
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);
			logger.info("---- Iniciando Tarea ----");
			
			ReadFile readFile = new ReadFile();
			readFile.listFiles();
			
			fh.close();

		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
