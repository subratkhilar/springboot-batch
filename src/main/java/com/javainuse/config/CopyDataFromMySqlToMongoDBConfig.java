package com.javainuse.config;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import com.javainuse.bean.Employee;

@Configuration
public class CopyDataFromMySqlToMongoDBConfig {
	private static final Logger logger = LoggerFactory.getLogger(CopyDataFromMySqlToMongoDBConfig.class);
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	@Autowired
	private DataSource dataSource;
	@Autowired
	private MongoTemplate mongoTemplate;
	private static final String QUERY_FIND_EMPLOYEES = "SELECT first_name, last_name,age,salary,address FROM employee ";

	@Bean
	public ItemReader<Employee> readerDataFromSql() {
		JdbcCursorItemReader<Employee> databaseReader = new JdbcCursorItemReader<Employee>();
		databaseReader.setDataSource(dataSource);
		databaseReader.setSql(QUERY_FIND_EMPLOYEES);
		databaseReader.setRowMapper(new BeanPropertyRowMapper<>(Employee.class));

		return databaseReader;
	}

	@Bean
	public Job sqlToMongo() {
		logger.info(" sqlToMongo >>>>>>>>>>> sqlToMongo");
		return jobBuilderFactory.get("sqlToMongo").incrementer(new RunIdIncrementer()).start(processSqlToMongo())
				.build();
	}

	@Bean
	public Step processSqlToMongo() {
		logger.info(" processSqlToMongo >>>>>>>>>>> processSqlToMongo");
		return stepBuilderFactory.get("processSqlToMongo").<Employee, Employee>chunk(10).reader(readerDataFromSql())
				.writer(sqlToMongoDBwriter()).build();
	}

	@Bean
	public MongoItemWriter<Employee> sqlToMongoDBwriter() {
		MongoItemWriter<Employee> writer = new MongoItemWriter<Employee>();
		writer.setTemplate(mongoTemplate);
		writer.setCollection("employee");
		return writer;
	}

}
