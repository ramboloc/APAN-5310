-- APAN 5310: SQL & RELATIONAL DATABASES

   -------------------------------------------------------------------------
   --                                                                     --
   --                            HONOR CODE                               --
   --                                                                     --
   --  I affirm that I will not plagiarize, use unauthorized materials,   --
   --  or give or receive illegitimate help on assignments, papers, or    --
   --  examinations. I will also uphold equity and honesty in the         --
   --  evaluation of my work and the work of others. I do so to sustain   --
   --  a community built around this Code of Honor.                       --
   --                                                                     --
   -------------------------------------------------------------------------

/*
 *    You are responsible for submitting your own, original work. We are
 *    obligated to report incidents of academic dishonesty as per the
 *    Student Conduct and Community Standards.
 */


-------------------------------------------------------------------------------
-------------------------------------------------------------------------------


-- HOMEWORK ASSIGNMENT 4

/*
 *  NOTES:
 *
 *    - Type your SQL statements between the START and END tags for each
 *      question. Do not alter this template .sql file in any way other than
 *      adding your answers. Do not delete the START/END tags. The .sql file
 *      you submit will be validated before grading and will not be graded if
 *      it fails validation due to any alteration of the commented sections.
 *
 *    - Our course is using PostgreSQL which has been preinstalled for you in
 *      Codio. We grade your assignments in PostgreSQL. You risk losing points
 *      if you prepare your SQL queries for a different database system
 *      (MySQL, MS SQL Server, Oracle, etc).
 *
 *    - It is highly recommended that you insert additional, appropriate data
 *      to test the results of your queries. You do not need to include your
 *      sample data in your answers.
 *
 *    - Make sure you test each one of your answers. If a query returns an
 *      error it will earn no points.
 *
 *    - In your CREATE TABLE statements you must provide data types,
 *      primary/foreign keys and integrity constraints (if applicable).
 *
 *    - You may expand your answers in as many lines as you need between the
 *      START/END tags.
 *
 */



-------------------------------------------------------------------------------



/*
 * QUESTION 1 (7 points)
 * ----------------------------------------------------------------------------
 *
 * Assume a schema for a company that matches employees with skills with projects.
 * Provide the SQL statements that create the five (5) tables with data types
 * of your design. Implement integrity constraints (primary/foreign keys,
 * NOT NULL, UNIQUE, etc.). 

 You are to create a database diagram showing the tables and relationships between employees, skills and projects. 
 The following information needs to be maintained:

Employee -  employee full name,  date of birth, social security number,  full address, job title. 

Projects - project name,  description

Skills -  skill name, description

The following is known:

1) Employees can work on many projects and projects can have many employees
2) Employees can work on more than one project at a time
3) Employees may be pulled off projects and then put back on at a later date (hint: composite key in bridge table needs a date to be unique)
4) Employees can have many skills and skills can have many employees associated with it


Please submit a PDF document with picture of your completed diagram.

 *
 *
 * Type the CREATE TABLE statements in the order they have to be executed so
 * that there is no error in PostgreSQL. Keep in mind that integrity
 * constraints can be executed after the CREATE TABLE statements using the
 * ALTER statement. Expand the space between the START/END tags to fit all of
 * your CREATE TABLE statements.
 */
 
-- START ANSWER 1 --
drop table employee_in_project;
drop table employee_have_skill;
drop table employee;
drop table skills;
drop table project;
CREATE TABLE employee (
  employee_id int NOT NULL,
  employee_name varchar(50) NOT NULL,
  birth TIMESTAMP NOT NULL,
  social_security_number varchar(50) NOT NULL,
  address varchar(255) NOT NULL,
  job_title varchar(20) NOT NULL,
  PRIMARY KEY (employee_id)
);
CREATE TABLE project (
  project_id int NOT NULL,
  project_name varchar(255) NOT NULL,
  description varchar(255) DEFAULT NULL,
  PRIMARY KEY (project_id)
);
CREATE TABLE skills (
  skill_id int NOT NULL,
  skill_name varchar(50) NOT NULL,
  desctiption varchar(255) DEFAULT NULL,
  PRIMARY KEY (skill_id)
);

CREATE TABLE employee_have_skill (
  employee_id int NOT NULL,
  skill_id int NOT NULL,
	FOREIGN KEY (employee_id) REFERENCES employee(employee_id)
);

CREATE TABLE employee_in_project (
  employee_id int NOT NULL,
  project_id int NOT NULL,
	effective_date TIMESTAMP,
	term_date TIMESTAMP,
	FOREIGN KEY (employee_id) REFERENCES employee(employee_id)
);
INSERT INTO employee(employee_id, employee_name, birth, social_security_number, address, job_title) VALUES (1, 'a', '2022-10-19 19:55:40', '213', 'new york', '32');
INSERT INTO employee(employee_id, employee_name, birth, social_security_number, address, job_title) VALUES (2, 'b', '2022-10-19 19:55:40', '111', '11', '11');
INSERT INTO employee(employee_id, employee_name, birth, social_security_number, address, job_title) VALUES (3, 'c', '2022-10-19 19:55:40', '111', '22', '11');
INSERT INTO employee(employee_id, employee_name, birth, social_security_number, address, job_title) VALUES (4, 'd', '2022-10-19 19:55:40', '111', '11', '11');
INSERT INTO employee(employee_id, employee_name, birth, social_security_number, address, job_title) VALUES (5, 'e', '2022-10-19 19:55:40', '111', '11', '11');
INSERT INTO employee_have_skill(employee_id, skill_id) VALUES (1, 1);
INSERT INTO employee_have_skill(employee_id, skill_id) VALUES (1, 2);
INSERT INTO employee_have_skill(employee_id, skill_id) VALUES (2, 1);
INSERT INTO employee_have_skill(employee_id, skill_id) VALUES (2, 2);
INSERT INTO employee_have_skill(employee_id, skill_id) VALUES (3, 1);
INSERT INTO employee_have_skill(employee_id, skill_id) VALUES (5, 1);
INSERT INTO employee_have_skill(employee_id, skill_id) VALUES (5, 2);
INSERT INTO employee_in_project(employee_id, project_id, effective_date, term_date) VALUES (1, 1, '2022-10-03 19:55:38', '2022-10-19 19:55:40');
INSERT INTO employee_in_project(employee_id, project_id, effective_date, term_date) VALUES (2, 1, '2022-10-01 19:55:48', '2022-10-12 19:55:52');
INSERT INTO employee_in_project(employee_id, project_id, effective_date, term_date) VALUES (3, 1, '2022-10-01 20:46:38', '2022-10-12 20:46:41');
INSERT INTO employee_in_project(employee_id, project_id, effective_date, term_date) VALUES (4, 2, '2022-10-03 20:46:54', '2022-10-13 20:46:58');
INSERT INTO employee_in_project(employee_id, project_id, effective_date, term_date) VALUES (1, 2, '2022-10-07 20:47:39', '2022-10-13 20:47:42');
INSERT INTO project(project_id, project_name, description) VALUES (1, 'HIPPA Update', 'sad');
INSERT INTO project(project_id, project_name, description) VALUES (2, '22', 'sad');
INSERT INTO skills(skill_id, skill_name, desctiption) VALUES (1, 'R', 'sdasd');
INSERT INTO skills(skill_id, skill_name, desctiption) VALUES (2, 'Python', 's');

-- END ANSWER 1 --

-------------------------------------------------------------------------------

/*
 * QUESTION 2 (3 points)
 * ---------------------
 *
 * Provide brief reasoning on your selection of each one of the data types
 * above as well as your implementation of integrity constraints. Include any
 * additional assumptions you made beyond the provided schema description. Type
 * your answers (plain text) within the START/END tags. Expand your answers in
 * as many lines as you need.
 *
 */

-- START ANSWER 2 --
/*


- The type of `employee_id`,`project_id` and `skill_id` are int and NOT NULL ,
- The type of `name` is varchar(50) and NOT NULL,
- The type of `birth` is varchar(20) and NOT NULL,
- The type of `social_security_number` varchar(50) and  NOT NULL to represent social code,
- The type of `address` is varchar(255) and  NOT NULL,
- The type of `job_title` is varchar(20) and NOT NULL,
- The type of `join_time` is TIMESTAMP to indicate the time when the employee joined the project,
- The type of `join_time` is TIMESTAMP to indicates the end time of the term

employee_id employee_have_skill and employee_in_project associate with table employee as a foreign key
project_id in table employee_in_project associate with table project as a foreign key


-- END ANSWER 2 --

-------------------------------------------------------------------------------

/*
 * QUESTION 3 (2 points)
 * ---------------------
 *
 * Draw the ER diagram for the schema detailed in Question 1 using Engineering
 * notation. Pay close attention to properly defining relationship and
 * cardinalities. You may draw the ER diagram in any software you prefer,
 * Lucidchart is recommended. Hand drawn diagrams will not be accepted. Upload
 * the ER diagram as a separate file.
 *
 */

 -- No START/END tags here. Your answer is a separate PDF submitted along with
 -- this SQL file and the PDF file for the diagram in Question 3.

-------------------------------------------------------------------------------

/*
 * QUESTION 4 (2 points)
 * ---------------------
 *
 * Provide the SQL statement that returns the projectid, projectname, employeename 
 * and effective and term dates of employees on the project named "HIPPA Update"  
 *
 *
 * IMPORTANT NOTE: This question must be answered with a single SQL query. If
 *                 your answer involves more that one query, it will be marked
 *                 incorrect even if the output is correct.
 *
 */

-- START ANSWER 4 --

select project.project_id,project.project_name,employee.employee_name,
employee_in_project.effective_date,employee_in_project.term_date
from employee_in_project join project on employee_in_project.project_id=project.project_id join 
employee on employee_in_project.employee_id=employee.employee_id
where project.project_name='HIPPA Update'

-- END ANSWER 4 --

-------------------------------------------------------------------------------

/*
 * QUESTION 5 (2 points)
 * ---------------------
 *
 * Provide the SQL statement that creates a view called v_NY_Employees. This
 * view, when called, must return the EmployeeID, Title, any projects assigned
 * to that employee and effective and term dates for employees located in new york. 
 *
 * Demonstrate how the view may be used to further limit results to projects
 * employees are currently working on  (hint: term date will be null)
 *
 * IMPORTANT NOTE: There are two parts to this question. First, you must write
 *                 the query that creates the view and then you must write the
 *                 query that uses this view to limit results to projects 
 *                 currently being worked on.
 *
 */

-- START ANSWER 5 --
CREATE VIEW v_NY_Employees as 
(SELECT employee.employee_id,employee.job_title,project.project_name,
employee_in_project.effective_date,employee_in_project.term_date
from employee_in_project join employee on employee.employee_id = employee_in_project.employee_id 
join project on employee_in_project.project_id=project.project_id 
where employee.address='new york');
/* test for view*/
select * from v_NY_Employees;
/* Use views to limit query results for items */
select * from project join v_NY_Employees on project.project_name=v_NY_Employees.project_name
-- END ANSWER 5 --

-------------------------------------------------------------------------------

/*
 * QUESTION 6 (2 points)
 * ---------------------
 *
 * Provide the SQL statement that returns the empolyee title that has the 
 * most employees with that title. (Hint: Order descending limt rows to 1)
 *
 * IMPORTANT NOTE: This question must be answered with a single SQL query. If
 *                 your answer involves more that one query, it will be marked
 *                 incorrect even if the output is correct.
 *
 */

-- START ANSWER 6 --
SELECT * from (SELECT job_title,count(*) as num from employee GROUP BY job_title) as temp
ORDER BY temp.num Desc limit 1


-- END ANSWER 6 --

-------------------------------------------------------------------------------

/*
 * QUESTION 7 (3 points)
 * ---------------------
 *
 * Provide the SQL statement that returns the names and number of projects of all employee(s) with 
 * the most number of projects
 *
 * IMPORTANT NOTE: This question must be answered with a single SQL query. If
 *                 your answer involves more that one query, it will be marked
 *                 incorrect even if the output is correct.
 */

-- START ANSWER 7 --
SELECT employee.employee_name,temp.project_num from employee join
(SELECT employee_id,COUNT(*) as project_num FROM employee_in_project GROUP BY employee_id) as temp
on employee.employee_id=temp.employee_id
-- END ANSWER 7 --

-------------------------------------------------------------------------------

/*
 * QUESTION 8 (2 points)
 * ---------------------
 *
 * Provide the SQL statement that returns names, title and skills of all employees who are
 * experienced in skills named "R" or "Python" (You may use the "IN" Operator) and are not on any projects (You may use the "NOT IN" Operator). 
 *
 * IMPORTANT NOTE: This question must be answered with a single SQL query. If
 *                 your answer involves more that one query, it will be marked
 *                 incorrect even if the output is correct.
 *
 */

-- START ANSWER 8 --
Select * from employee where address like 'new%';
SELECT * FROM (SELECT employee.employee_id,array_agg(skills.skill_name) as skills_ FROM employee_have_skill JOIN skills
on employee_have_skill.skill_id=skills.skill_id join employee
on employee_have_skill.employee_id=employee.employee_id 
GROUP BY employee.employee_id) as temp 
WHERE temp.skills_::text like '%R%' and temp.skills_::text like '%Python%' 
and employee_id not in (SELECT employee_id from employee_in_project)

-- END ANSWER 8 --

-------------------------------------------------------------------------------

/*
 * QUESTION 9 (3 points)
 * ---------------------
 *
 * Provide the SQL statement that lists employees, their age, and their skill that are not on any projects 
 * (HINT: NOT IN OPERATOR)
 * 
 * IMPORTANT NOTE: This question must be answered with a single SQL query. If
 *                 your answer involves more that one query, it will be marked
 *                 incorrect even if the output is correct.
 *
 */

-- START ANSWER 9 --

SELECT employee.employee_name,(current_date-birth) as age from employee

-- END ANSWER 9 --

-------------------------------------------------------------------------------

/*
 * QUESTION 10 (4 points)
 * ----------------------
 *
 * Write a table function named 'skills_needed_for' that returns all skills associated
 * with employees for the given project name. The function should not return a table with employees, just 
 * the project name and skills.
 *
 *
 */

-- START ANSWER 10 --

/*set global log_bin_trust_function_creators=TRUE;*/


CREATE OR REPLACE FUNCTION skills_needed_for(t_project_name varchar(50)) 
	returns varchar(255) as 
	$func$
		begin

			return (SELECT CONCAT(t_project_name,': ',array_agg(temp.skill_name)) from 
			(select DISTINCT skills.skill_name from employee_in_project join employee 
			on employee_in_project.employee_id=employee.employee_id join employee_have_skill 
			on employee_have_skill.employee_id=employee.employee_id join project 
			on project.project_id=employee_in_project.project_id join skills
			on skills.skill_id=employee_have_skill.skill_id
			where project.project_name=t_project_name) as temp);                  
		end
	$func$
language plpgsql;




-- END ANSWER 10 --

-------------------------------------------------------------------------------



/*
 * QUESTION 11 (1 points)
 * ----------------------
 *
 * Write a query that uses the table function created above in the question-10. 
 *
 *
 */

-- START ANSWER 11 --

SELECT skills_needed_for('HIPPA Update')

-- END ANSWER 11 --

