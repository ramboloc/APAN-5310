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


-- HOMEWORK ASSIGNMENT 3 SOLUTIONS

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
 * QUESTION 1 (8 points: 1 point for each table plus 1 point for correct order
 *             of execution)
 * ----------------------------------------------------------------------------
 *
 * You are tasked to create a simplified database of a database for a food delivery app. 
 * Provide the SQL statements that create the tables with data types of your design.
 * Implement integrity constraints (primary/foreign keys, NOT NULL) as needed.
 * Note: since underlining is not supported in this file format, primary keys
 * for each relation below are shown within '*'.
 *
 *   customer_address (*id, customer_id, address_id)
 *   customer (*customer_id, first_name, last_name, email, phone, time_joined)
 *   address (*address_id,, street, city, state, zip)
 *   placed_order (*order_id, restaurant_id, customer_id, order_time, total_price)
 *   in_order_item (*in_order_item_id, order_id, menu_item_id, quantity)
 *   menu_item (*menu_item_id*, item_name, restaurant_id, description, price, is_active)
 *   restaurant (*restaurant_id, restaurant_name, cuisine, address_id)
 *
 *
 * Type the CREATE TABLE statements in the order they have to be executed so
 * that there is no error in PostgreSQL. Expand the space between the START/END
 * tags to fit all of your CREATE TABLE statements.
 *
 * IMPORTANT: Make sure to implement the schema with exactly the provided
 *            relation and attribute names. Do not rename relations or
 *            attributes, either by accident (typos) or on purpose.
 *
 *
 */
-- START ANSWER 1 --
drop TABLE in_order_item;
drop TABLE placed_order;
drop TABLE menu_item;
drop TABLE restaurant;
drop TABLE customer_address;
drop TABLE customer;
drop TABLE address;

CREATE TABLE customer(
	customer_id int not null,
	first_name varchar(50) not null, 
	last_name varchar(50) not null, 
	email varchar(50) not null, 
	phone varchar(50) not null, 
	time_joined TIMESTAMP,
	PRIMARY KEY(customer_id)
);

CREATE TABLE address(
	address_id int not null,
	street varchar(50) not null, 
	city varchar(50) not null, 
	state varchar(50) not null, 
	zip VARCHAR(50),
	PRIMARY KEY(address_id)
);

CREATE TABLE customer_address(
	id int not null,
	customer_id int not null,
	address_id int not null,
	PRIMARY KEY(id),
	FOREIGN KEY(customer_id) REFERENCES customer(customer_id),
	FOREIGN KEY(address_id) REFERENCES address(address_id)
);

 
 CREATE TABLE restaurant(
 restaurant_id int not null, 
 restaurant_name VARCHAR(50) not null, 
 cuisine VARCHAR(50) not null, 
 address_id int not null,
 PRIMARY KEY(restaurant_id),
 FOREIGN KEY(address_id) REFERENCES address(address_id)
 );

CREATE TABLE placed_order(
	order_id int not null, 
	restaurant_id int not null, 
	customer_id int not null, 
	order_time int, 
	total_price DOUBLE,
	PRIMARY KEY(order_id),
	FOREIGN KEY(customer_id) REFERENCES customer(customer_id),
	FOREIGN KEY(restaurant_id) REFERENCES restaurant(restaurant_id)
);

 
 CREATE TABLE menu_item(
  menu_item_id int not null, 
	item_name int not null, 
	restaurant_id int not null, 
	description VARCHAR(255) not null,  
	price DOUBLE not null, 
	is_active int not null,
	PRIMARY KEY(menu_item_id),
	FOREIGN KEY(restaurant_id) REFERENCES restaurant(restaurant_id),
	CONSTRAINT check_is_active CHECK(is_active=1 or is_active=0)
	);

CREATE TABLE in_order_item(
 in_order_item_id int not null, 
 order_id int not null,
 menu_item_id int not null,
 quantity int,
 PRIMARY KEY(in_order_item_id),
 FOREIGN KEY(order_id) REFERENCES placed_order(order_id),
 FOREIGN KEY(menu_item_id) REFERENCES menu_item(menu_item_id)
 );


-- END ANSWER 1 --

-------------------------------------------------------------------------------

/*
 * QUESTION 2 (4 points)
 * ---------------------
 *
 * Provide detailed reasoning on your selection of each one of the data types
 * above as well as your implementation of any/all integrity constraints. Include
 * any additional assumptions you made beyond the provided schema description.
 * Explain relationships and cardinalities. Type your answers as plain text
 * within the START/END tags. Expand your answer in as many lines as you need.
 *
 */

-- START ANSWER 2 --
/*
For the id field of each table, we use the int type as its unique identifier.
For the customer's name, email, restaurant and address, it is obvious that this should be a character type. 
For the telephone, we can take the design as the character type. 
For order_ We set price and price in the restaurant to double for comparison. 
TABLE customer(
	customer_id: int, 
	first_name :varchar(50) , The name should be of character type
	last_name: varchar(50), The name should be of character type
	email: varchar(50), The email should be of character type
	phone: varchar(50), The phone should be of character type
	time_joined: TIMESTAMP, We use TIMESTAMP to express time
	PRIMARY KEY(customer_id)
);

TABLE address(
	address_id: int,
	street: varchar(50), 
	city: varchar(50), 
	state: varchar(50), 
	zip: VARCHAR(50), ZIP Code should be a continuous string of characters
	PRIMARY KEY(address_id)
);

TABLE customer_address(
	id int not null,
	customer_id int not null,
	address_id int not null,
	PRIMARY KEY(id),
	FOREIGN KEY(customer_id) REFERENCES customer(customer_id),
	FOREIGN KEY(address_id) REFERENCES address(address_id)
);

 
TABLE restaurant(
 restaurant_id int not null, 
 restaurant_name VARCHAR(50), represents the name of the restaurant, character type
 cuisine VARCHAR(255), can be represented by a json string
 address_id int not null,
 PRIMARY KEY(restaurant_id),
 FOREIGN KEY(address_id) REFERENCES address(address_id)
 );

TABLE placed_order(
	order_id int not null, 
	restaurant_id int not null, 
	customer_id int not null, 
	order_time int,  Represents the consumption in that year
	total_price DOUBLE,
	PRIMARY KEY(order_id),
	FOREIGN KEY(customer_id) REFERENCES customer(customer_id),
	FOREIGN KEY(restaurant_id) REFERENCES restaurant(restaurant_id)
);

 
TABLE menu_item(
  menu_item_id int not null, 
	item_name int not null, 
	restaurant_id int not null, 
	description VARCHAR(255),  Description
	price DOUBLE, Double type represents price, which is convenient for us to calculate and compare
	is_active int not null, This should be 0 or 1, indicating whether it is active
	PRIMARY KEY(menu_item_id),
	FOREIGN KEY(restaurant_id) REFERENCES restaurant(restaurant_id)
	);

TABLE in_order_item(
 in_order_item_id int not null, 
 order_id int not null,
 menu_item_id int not null,
 quantity int, Indicates quantity
 PRIMARY KEY(in_order_item_id),
 FOREIGN KEY(order_id) REFERENCES placed_order(order_id),
 FOREIGN KEY(menu_item_id) REFERENCES menu_item(menu_item_id)
 );

A foreign key represents a correlation between two relationships. 
All constraints with foreign keys here actually represent a relationship.
is_active field should have only two values 0 or 1.

*/

-- END ANSWER 2 --

-------------------------------------------------------------------------------

/*
 * QUESTION 3 (6 points)
 * ----------------------
 *
 * Draw the ER diagram for the schema detailed in Question 1 using any
 * notation as in your textbook / what we presented in the slides and 
 * lecture for this module. Pay close attention in properly
 * defining relationships and cardinalities. Points may be deducted for
 * incorrect relationship and/or incorrect cardinalities.
 * You may draw the ER diagram in any software you prefer. 
 * Hand drawn diagrams will not be accepted. 
 * Upload the ER diagram as a separate file.
 *
 */

-- No START/END tags here. Your answer is a separate PDF submitted along with
-- this SQL file.


-------------------------------------------------------------------------------

/* * QUESTION 4 (3 point) 
* -------------------- * 
* Which restaurant is the most popular one? 
* Provide the SQL statement that returns the total revenue (sum of price on all orders)
* for each restaurant in 2021, ranked by revenue (hint: use GROUP BY)

* */-- START ANSWER 4 --
/*We calculate the total consumption of all customers in each restaurant.
Then we find the restaurant ID with the highest income in this query result.
*/
SELECT total_revenue.restaurant_id,MAX(total_revenue.revenue) from 
(SELECT restaurant_id,SUM(total_price) as revenue FROM placed_order where order_time=2021  GROUP BY restaurant_id) as total_revenue

-- END ANSWER 4 --
-------------------------------------------------------------------------------------------------
