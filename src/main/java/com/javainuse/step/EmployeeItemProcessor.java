package com.javainuse.step;

import org.springframework.batch.item.ItemProcessor;

import com.javainuse.bean.Employee;

public class EmployeeItemProcessor implements ItemProcessor<Employee, Employee> {

	@Override
	public Employee process(Employee employee) throws Exception {
		
		return employee;
	}

}
