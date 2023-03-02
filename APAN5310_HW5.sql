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


-- HOMEWORK ASSIGNMENT 5

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
 *
 * NOTE: For Questions 1 through 4, you need to set up your database and populate the following table:
 *
 *          weather_data (station, name, date, time, hourly_temp, hourly_wind)
 *
 *       The CREATE TABLE and INSERT INTO statements for this schema are
 *       provided in a separate file on the HW5 coursework page.
 *
 *
 * QUESTION 1 (2 points)
 * ---------------------
 * Provide the SQL statement that returns the station, name, date, time, hourly_temp
 * and rank of hourly temperatures. Display the results in proper rank order.
 * The hottest reading across all stations should be ranked first and the coldest
 * coldest last. Apply dense ranking.
 *
 */

-- START ANSWER 1 --
select station, name, date, time, hourly_temp,DENSE_RANK() OVER (
        ORDER BY hourly_temp DESC
    ) as my_rank from weather_data 


-- END ANSWER 1 --

-------------------------------------------------------------------------------

/*
 * QUESTION 2 (2 points)
 * ---------------------
 * Provide the SQL statement that returns the station, name, date, time, hourly_temp
 * and rank of hourly temperatures for each station. Display the results in
 * proper rank order. The hottest reading for each station be ranked first and
 * the coldest last. Apply normal ranking. Sort by station name and rank.
 *
 */

-- START ANSWER 2 --

select station, name, date, time, hourly_temp,
RANK() OVER (ORDER BY hourly_temp DESC) as my_rank 
from weather_data 


-- END ANSWER 2 --

-------------------------------------------------------------------------------

/*
 * QUESTION 3 (2 points)
 * ---------------------
 * Provide the SQL statement that returns the station, name, date, time and 
 * hourly wind speeds of the 20 highest windspeed ever recorded. Notice you may have 
 * more than 20 rows in the result due to ties.
 *
 */

-- START ANSWER 3 --

select * from (select station, name, date, time, hourly_wind,
RANK() OVER (ORDER BY name, hourly_temp DESC) as my_rank 
from weather_data) as temp 
where temp.my_rank<=20

-- END ANSWER 3 --

-------------------------------------------------------------------------------

/*
 * QUESTION 4 (2 points)
 * ---------------------
 * Provide the SQL statement that returns the station, name, date 
 * and the daily average temperature per station per day.
 *
 */

-- START ANSWER 4 --
select station, name, date, AVG(hourly_temp)
from weather_data GROUP BY station,name,date


-- END ANSWER 4 --

-------------------------------------------------------------------------------

/*
 * QUESTION 5 (8 points)
 * ---------------------
 * Download the "APAN 5310 HW5 Normalization" excel file
 *
 *
 * The table shown is a simplified scenario of a movie rental store. Each row
 * contains information about the customer and the movie(s) they rented. Notice
 * that a customer may rent more than one movies at a time. Your task is to
 * produce a relational design up to third normal form (3NF). Show all work and
 * add comments. Show the tables at each step with the data. Remember that you
 * may not need to make changes at every normal form step if the rule is
 * satisfied.
 *
 */

 -- No START/END tags here. Your answer is a separate PDF submitted along with
 -- this SQL file.

-------------------------------------------------------------------------------
