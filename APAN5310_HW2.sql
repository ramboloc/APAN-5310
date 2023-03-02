-- SQL & RELATIONAL DATABASES SPRING 2022

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


/*
 *  NOTES:
 *
 *    - Type your SQL statements between the START and END tags for each
 *      question, as shown in the example. Do not alter this template .sql file
 *      in any other way other than adding your answers. Do not delete the
 *      START/END tags. The .sql file you submit will be validated before
 *      grading and will not be graded if it fails validation due to any
 *      alteration of the commented sections.
 *
 *    - Our course is using PostgreSQL which has been preinstalled for you in
 *      Codio. We grade your assignments in Codio and PostgreSQL. You risk
 *      losing points if you prepare your SQL queries for a different database
 *      system (MySQL, MS SQL Server, Oracle, etc).
 *
 *    - It is highly recommended that you insert additional, appropriate data
 *      to test the results of your queries. You do not need to include your
 *      sample data in your answers.
 *
 *    - Make sure you test each one of your answers in pgAdmin. If a query
 *      returns an error it will earn no points.
 *
 */


 /* *********************I M P O R T A N T***************************/
 /*																	*/
 /*						SCORING RULES for HW2						*/
 /*						---------------------						*/
 /*																	*/
 /*																	*/
 /*	Your sql statement can be different than the official solution  */
 /*	that will be released, as far as (1) it generates exact expected*/
 /* answer for any dataset, and (2) executes without any error		*/
 /*																	*/
 /*	Your score for each question will be either 1 or 0				*/
 /*	No partial score for any question								*/
 /*																	*/
 /* All questions are made as much possible as unambiguous. However,*/
 /* If you have any 'assumption' made for your sql statement then   */
 /* it is your task to note that as a comment in the solution	    */
 /*																	*/
 /* *********************I M P O R T A N T***************************/
 
 

/*
 * EXAMPLE
 * -------
 *
 * Provide the SQL statement that returns all attributes and tuples from
 * a relation named "table1".
 *
 */

-- START EXAMPLE --

--SELECT * FROM table1;

-- END EXAMPLE --



-------------------------------------------------------------------------------



/*
 * QUESTION 1 (1 point)
 * --------------------
 *
 * Provide the SQL statement that creates a table named "cars" with the
 * following attributes: "id" (typical integer),
 * "make" (variable length character string of length 30), "model" (variable
 * length character string of length 30), "type" (variable length character
 * string of length 20), "release_year" (small-range integer),
 * "width" (fixed point number with 4 digits, 1 decimal), "length" (fixed point
 * number with 4 digits, 1 decimal), "height" (fixed point number with 4 digits,
 * 1 decimal), "horsepower" (small-range integer), "total_production" (typical
 * integer), "description" (variable unlimited length), "maxacceleration" (small-range integer), "mpg" (numeric), and "last_update"
 * (timestamp). Do not implement integrity constraints.
 *
 */

-- START ANSWER 1 --
CREATE TABLE `cars`(
	`id` int,
	`make` VARCHAR(30),
	`model` VARCHAR(30),
	`type` VARCHAR(20),
	`year` SMALLINT,
	`width` NUMERIC(4,1),
	`length` NUMERIC(4,1),
	`height` NUMERIC(4,1),
	`horsepower` SMALLINT,
	`total_production` INT,
	`description` text,
	`acceleration` SMALLINT,
	`mpg` NUMERIC,
	`last_update` TIMESTAMP,
	PRIMARY KEY (ID)
)

-- END ANSWER 1 --

-------------------------------------------------------------------------------

/*
 * QUESTION 2 (1 point)
 * --------------------
 *
 * Provide the SQL statement that populates the "cars" table with values as
 * shown in the follwong table. (Note: try to run this in one statement, not 
 * multiple INSERT statements)
 *
 * +-----+------------+-----------+-------------+------+-------+--------+--------+------------+------------------+---------------------------+---------------------+---------------------------+---------------------+
 * | id  | make       | model     | type        | year | width | length | height | horsepower | total_production | description               | last_update         | acceleration             | mpg                |
 * +-----+------------+-----------+-------------+------+-------+--------+--------+------------+------------------+---------------------------+---------------------+---------------------------+---------------------+
 * | 1   | Audi       | A4        | Compact     | 2016 | 66.2  | 176.6  | 54.3   | 109        | 230686           | The Audi A4 is a line of  | 2021-04-25 10:34:15 | 		24.5		     | 44 |
 * |     |            |           |             |      |       |        |        |            |                  | compact executive cars    |                     | 						 |    |
 * |     |            |           |             |      |       |        |        |            |                  | produced since ...        |                     |                         |                    
 * +-----+------------+-----------+-------------+------+-------+--------+--------+------------+------------------+---------------------------+---------------------+---------------------------+---------------------+
 * | 2	 | Mitsubishi | Outlander | Compact SUV | 2020 | 67.6  | 179.2  | 70.1   | 185        | 56162            |                           | 2021-06-01 03:05:37 |        18.4             | 30 |
 * +-----+------------+-----------+-------------+------+-------+--------+--------+------------+------------------+---------------------------+---------------------+---------------------------+---------------------+
* | 3	 | GMC 		  | Yukon 	  | Large SUV   | 2015 | 80.1  | 183.7  | 73.0   | 315        | 1000             |                           | 2021-01-01 00:00:00 |        15.6             | 22 |
 * +-----+------------+-----------+-------------+------+-------+--------+--------+------------+------------------+---------------------------+---------------------+---------------------------+---------------------+
 * | 4	 | Ford       | Taurus    | Mid-size    | 2017 | 67.2  | 178.0  | 58.6   | 350        |                  | 						     | 2021-12-16 16:51:34 | 		16.5		     | 25|
 * +-----+------------+-----------+-------------+------+-------+--------+--------+------------+------------------+---------------------------+---------------------+---------------------------+---------------------+
 * | 5	 | Ford       | Fiesta    | Compact     | 2016 | 65.2  | 175.0  | 53.6   | 109        | 88000            |  					     | 2021-12-16 11:11:34 |  		30.6			| 42 |
 * +-----+------------+-----------+-------------+------+-------+--------+--------+------------+------------------+---------------------------+---------------------+---------------------------+---------------------+
 * | 6	 | Ford       | Focus     | Sub Compact | 2016 | 61.2  | 170.0  | 52.6   | 99         | 8000             |  					     | 2020-12-16 11:11:34 |  		30.5		    | 37 |
 * +-----+------------+-----------+-------------+------+-------+--------+--------+------------+------------------+---------------------------+---------------------+---------------------------+---------------------+
 * | 7	 | Ford       | Focus     | Sub Compact | 2015 | 61.2  | 170.0  | 52.6   | 99         | 9000             |  					     | 2020-12-16 11:11:34 |  		30.5		    | 37  |
 * +-----+------------+-----------+-------------+------+-------+--------+--------+------------+------------------+---------------------------+---------------------+---------------------------+---------------------+
 *
 */

-- START ANSWER 2 --
INSERT INTO cars VALUES(1,'Audi','A4','Compact',2016,66.2,176.6,54.3,109,230686,
'The Audi A4 is a line of compact executive cars produced since ...',24.5,44,null),
(2,'Mitsubishi ','Outlander ','Compact SUV',2020,67.6,179.2,70.1,185,56162,null,18.4,30,null),
(3,'GMC ','Yukon ','Large SUV',2015,80.1,183.7,73.0,315,1000,null,15.6,22,null),
(4,'Ford','Taurus','Mid-size',2017,67.2,178.0,58.6,350,null,null,16.5,25,null),
(5,'Ford ','Fiesta','Compact',2016,65.2,175.0,53.6,109,88000,null,30.6,42,null),
(6,'Ford ','Focus','Sub Compact',2016,61.2,170.0,52.6,99,8000,null,30.5,37,null),
(7,'Ford ','Focus','Sub Compact',2015,61.2,170.0,52.6,99,9000,null,30.5,37,null)


-- END ANSWER 2 --

-------------------------------------------------------------------------------

/*
 * QUESTION 3 (1 point)
 * --------------------
 *
 * Provide the SQL statement that returns all attributes and tuples from table
 * "cars", of all SUV types, whose fuel efficincy is greater than 20 miles per gallon. 
 *
 */

-- START ANSWER 3 -- 
SELECT * FROM cars WHERE type like "%SUV%" and mpg>20
 
-- END ANSWER 3 --

-------------------------------------------------------------------------------

/*
 * QUESTION 4 (1 point)
 * --------------------
 *
 * Assume that the width, length and height attributes in the "cars" relation
 * are in inches. Provide the SQL statement that returns the id, make and model as well as width, length and height attributes in millimeters
 * (note: 1 inch = 25.4 mm). Rename width, length and height to width_mm,
 * length_mm and height_mm respectively.
 *
 */

-- START ANSWER 4 --
SELECT id,make,model,width*25.4 as width_mm,length*25.4 as length_mm ,height*25.4 as height_mm FROM cars

-- END ANSWER 4 --

-------------------------------------------------------------------------------

/*
 * QUESTION 5 (1 point)
 * --------------------
 *
 * Provide the SQL statement that returns the "model" and "total_production"
 * attributes of all Ford vehicles with type that starts with the word "Compact"
 * of year 2015 or later.
 *
 */

-- START ANSWER 5 --
SELECT model,total_production from cars where make= 'Ford' and type like 'Compact%' and `year`>=2015

-- END ANSWER 5 --

-------------------------------------------------------------------------------

/*
 * QUESTION 6 (1 point)
 * --------------------
 *
 * a) Provide the SQL statement that creates a relation named "factories" with
 * attributes "factory_id" (typical integer), "company_name" (variable length
 * character string of length 30), "country" (variable length character string
 * of length 20). Do not implement integrity constraints.
 *
 * b) Provide the SQL statment to insert a record for factory_id, 1 where company name is Mitsubishi and country is Spain */

-- START ANSWER 6 --

CREATE TABLE factories(
	`factory_id` int,
	`company_name` VARCHAR(30),
	`country` VARCHAR(20)
)
INSERT INTO factories VALUES(1,'Mitsubishi','Spain')

-- END ANSWER 6 --

-------------------------------------------------------------------------------

/*
 * QUESTION 7 (1 point)
 * --------------------
 *
 * Provide the SQL statement that alters relation "cars" adding an attribute
 * called "msrp" (fixed point number with 8 digits, 2 decimals).
 *
 */

-- START ANSWER 7 --

ALTER TABLE cars ADD COLUMN msrp NUMERIC(8,2) AFTER mpg
-- END ANSWER 7 --

-------------------------------------------------------------------------------

/*
 * QUESTION 8 (1 point)
 * --------------------
 * Provide the SQL statement that returns the model and average msrp
 * of the model for Subaru made vehicles. (hint: use GROUP BY)

 *
 */

-- START ANSWER 8 --
SELECT model, AVG(msrp) from cars where make= 'Subaru' GROUP BY model

-- END ANSWER 8 --

-------------------------------------------------------------------------------

/*
 * QUESTION 9 (1 point)
 * --------------------
 *
 * Provide the SQL statement that returns the average msrp per vehicle make,
 * model and type. Name average msrp output column as "AveragePrice"
 *	(hint: use GROUP BY)
 *
 */

-- START ANSWER 9 --

SELECT AVG(msrp) as AveragePrice,model,type from cars GROUP BY type,model

-- END ANSWER 9 --

-------------------------------------------------------------------------------

/*
 * QUESTION 10 (1 point)
 * ---------------------
 *
 * Provide the SQL statement that returns the make, model, type and year of all
 * Mitsubishi vehicles manufactured in Spain. (Hint: we are not implementing joins
 * yet, use the cartesian product. Also, assume that the "make" in the
 * "cars" relation can correlate with the "company_name" attribute of the
 * "factories" relation).
 *
 */

-- START ANSWER 10 --
SELECT make,model,type,`year` FROM cars WHERE make='Mitsubishi'

-- END ANSWER 10 --

-------------------------------------------------------------------------------

/*
 * QUESTION 11 (1 point)
 * ---------------------
 *
 * Provide the SQL statement that returns the make, model, year, mpg and
 * country of production of all vehicles. Display the results by descending
 * year and ascending mpg.
 *
 */

-- START ANSWER 11 --

SELECT make,model,`year`,country FROM cars join factories on cars.make=factories.company_name

-- END ANSWER 11 --

-------------------------------------------------------------------------------

/*
 * QUESTION 12 (1 point)
 * ---------------------
 *
 * Provide the SQL statement that returns the make and total number of
 * vehicles produced for each make. Name the total column as "TotalProduced"
 *
 */

-- START ANSWER 12 --

SELECT make,total_production as TotalProduced from cars

-- END ANSWER 12 --

-------------------------------------------------------------------------------

/*
 * QUESTION 13 (1 point)
 * ---------------------
 *
 * Provide the SQL statement that returns the unique combination of models and 
 * horsepower from the cars relation.
 *
 */


-- START ANSWER 13 --
SELECT DISTINCT model,horsepower from cars

-- END ANSWER 13 --

-------------------------------------------------------------------------------

/*
 * QUESTION 14 (1 point)
 * ---------------------
 *
 * 100 more of 2020 Mitsubishi Outlander vehicles are produced by the company.
 * Update the vehicles table to add 100 into the appropriate total_production.
 *
 */

-- START ANSWER 14 --

UPDATE cars set total_production=total_production+100 WHERE make='Mitsubishi' and model='Outlander'

-- END ANSWER 14 --

-------------------------------------------------------------------------------

/*
 * QUESTION 15 (1 point)
 * ---------------------
 *
 * Provide the SQL statement that deletes all tuples from relation "cars"
 * that are made in USA with acceleration less than 16 .
 *
 */

-- START ANSWER 15 --
DELETE FROM cars where cars.acceleration<16

-- END ANSWER 15 --

-------------------------------------------------------------------------------

/* 
* QUESTION 16 (1 point)
* -------------------- 
*
* Provide the SQL statement that returns the id, make, model and total production 
* of highest produced models of each make, regardless of year. 
*/

-- START ANSWER 16--
SELECT id,make,model,total_production from cars INNER JOIN 
(SELECT make as max_make,model as max_model,MAX(total_production) as production from cars 
GROUP BY make,model) as max_production on max_production.production=cars.total_production and max_production.max_model=cars.model and max_production.max_make=cars.make

-- END ANSWER 16 --

-------------------------------------------------------------------------------

