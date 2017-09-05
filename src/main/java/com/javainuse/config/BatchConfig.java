package com.javainuse.config;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.javainuse.bean.Domain;
import com.javainuse.bean.Employee;
import com.javainuse.listener.JobCompletionListener;
import com.javainuse.step.EmployeeItemProcessor;
import com.javainuse.step.Processor;
import com.javainuse.step.Reader;
import com.javainuse.step.Writer;

@Configuration
public class BatchConfig {
	private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	@Autowired
	private DataSource dataSource;
	@Autowired
	private MongoTemplate mongoTemplate;

	// copy data from CSV and insert into mysql batch job begin
	@SuppressWarnings("unchecked")
	@Bean
	public FlatFileItemReader<Employee> reader() {

		FlatFileItemReader<Employee> reader = new FlatFileItemReader<Employee>();
		reader.setResource(new ClassPathResource("employee.CSV"));
		reader.setLineMapper(new DefaultLineMapper<Employee>() {
			{
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "firstName", "lastName", "age", "salary", "address" });
					}
				});
				setFieldSetMapper(new BeanWrapperFieldSetMapper<Employee>() {
					{
						setTargetType(Employee.class);
					}
				});
			}
		});
		logger.info("FlatFileItemReader >>>>>>>>>>> reader");
		return reader;
	}

	@Bean
	public EmployeeItemProcessor employeeProcessor() {
		return new EmployeeItemProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<Employee> writer() {
		JdbcBatchItemWriter<Employee> writer = new JdbcBatchItemWriter<Employee>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Employee>());
		writer.setSql(
				"INSERT INTO Employee (first_name, last_name,age,salary,address) VALUES (:firstName, :lastName,:age, :salary,:address)");
		// writer.setSql("INSERT INTO Employee (first_name, last_name) VALUES
		// (:firstName, :lastName)");
		writer.setDataSource(dataSource);
		logger.info("JdbcBatchItemWriter >>>>>>>>>>> writer");
		return writer;
	}

	@Bean
	public Job importUserJob(JobExecutionListener listener) {
		logger.info(" Job >>>>>>>>>>> importUserJob");
		return jobBuilderFactory.get("importUserJob").incrementer(new RunIdIncrementer()).listener(listener)
				.flow(step1()).end().build();
	}

	@Bean
	public Step step1() {
		logger.info(" Step >>>>>>>>>>> step1");
		return stepBuilderFactory.get("step1").<Employee, Employee>chunk(10).reader(reader())
				.processor(employeeProcessor()).writer(writer()).build();
	}

	// end
	// writting data into mongo db start
	@Bean
	public FlatFileItemReader<Domain> mongoDatareader() {
		FlatFileItemReader<Domain> reader = new FlatFileItemReader<>();
		reader.setResource(new ClassPathResource("sample-data.csv"));
		reader.setLineMapper(new DefaultLineMapper<Domain>() {
			{
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "id", "name" });

					}
				});
				setFieldSetMapper(new BeanWrapperFieldSetMapper<Domain>() {
					{
						setTargetType(Domain.class);
					}
				});
			}
		});
		logger.info(" mongoDatareader >>>>>>>>>>> mongoDatareader");
		return reader;
	}

	@Bean
	public Job readCSVFile() {
		logger.info(" readCSVFile >>>>>>>>>>> readCSVFile");
		return jobBuilderFactory.get("readCSVFile").incrementer(new RunIdIncrementer()).start(processMongoData()).build();
	}

	@Bean
	public Step processMongoData() {
		logger.info(" processMongoData >>>>>>>>>>> processMongoData");
		return stepBuilderFactory.get("processMongoData").<Domain, Domain>chunk(10).reader(mongoDatareader())
				.writer(mongoDBwriter()).build();
	}

	@Bean
	public MongoItemWriter<Domain> mongoDBwriter() {
		MongoItemWriter<Domain> writer = new MongoItemWriter<Domain>();
		writer.setTemplate(mongoTemplate);
		writer.setCollection("domain");
		return writer;
	}

	// end
	@Bean
	public Job processJob() {
		return jobBuilderFactory.get("processJob").incrementer(new RunIdIncrementer()).listener(listener())
				.flow(orderStep1()).end().build();
	}

	@Bean
	public Step orderStep1() {
		return stepBuilderFactory.get("orderStep1").<String, String>chunk(1).reader(new Reader())
				.processor(new Processor()).writer(new Writer()).build();
	}

	@Bean
	public JobExecutionListener listener() {
		return new JobCompletionListener();
	}

}
