# -- APAN 5310: SQL & RELATIONAL DATABASES SPRING 2022
#   -------------------------------------------------------------------------
#   --                                                                     --
#   --                            HONOR CODE                               --
#   --                                                                     --
#   --  I affirm that I will not plagiarize, use unauthorized materials,   --
#   --  or give or receive illegitimate help on assignments, papers, or    --
#   --  examinations. I will also uphold equity and honesty in the         --
#   --  evaluation of my work and the work of others. I do so to sustain   --
#   --  a community built around this Code of Honor.                       --
#   --                                                                     --
#   -------------------------------------------------------------------------


#     You are responsible for submitting your own, original work. We are
#     obligated to report incidents of academic dishonesty as per the
#     Student Conduct and Community Standards.


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------


# -- HOMEWORK ASSIGNMENT 6 (DUE: )


#   NOTES:
#
#     - Type your code between the START and END tags. Code should cover all
#       questions. Do not alter this template file in any way other than
#       adding your answers. Do not delete the START/END tags. The file you
#       submit will be validated before grading and will not be graded if it
#       fails validation due to any alteration of the commented sections.
#
#     - We grade your assignments in Pythor or R, and PostgreSQL. 
#		You risk losing points if you prepare your SQL queries for a different database
#       system (MySQL, MS SQL Server, Oracle, etc) or any script other than Python or R.
#
#     - Make sure you test each one of your answers. 
#		Each answer should contain sufficient statements such that 
#		while we grade your work, the solution of each answer can be copy/paste and executed 
#	    individually (in correct sequence -i.e. answer #2 might not run correctly before answer #1 was executed)
#		without any missing statement (i.e. the script for each answer must be self-sufficient)
#		, to generate desired result without any error.
#
#
#     - You may expand your answers in as many lines as you find appropriate
#       between the START/END tags.
#


# -----------------------------------------------------------------------------
#
#  NOTE: Provide the script that covers all questions between the START/END tags
#        at the end of the file. No separate START/END tags for each question.
#        Add comments to explain each step.
#
#
#  QUESTION 1 (10 points)
#  -----------------------------------------------------
#  For this assignment we will use a dataset of laptop 
#  company price list. The dataset provides a snapshot of
#  laptop pricing and not the whole picture.
#  Meaning, the dataset has both new and existing laptops with prices
#  based on the existing company. Download the dataset from the
#  assignment page. Here is a quick overview of the dataset columns:
#
#    company:Laptop Manufacturer
#    product: Brand and Model
#    typename: Type (Notebook, Ultrabook, Gaming, etc.)
#    inches: Numeric- Screen Size
#    screen/resolution: Screen Resolution
#    cpu: Central Processing Unit (CPU)
#    ram: Laptop RAM
#	 memory: Hard Disk / SSD Memory
#    gpu: Graphics Processing Units (GPU)
#    opsys: Operating System
#	 weight: Laptop Weight
#	 price_euros: Price (Euro)
#  You will notice that there can be multiple prices per product, each
#  one recorded on a separate row.
#
#  Design an appropriate 3NF relational schema. Provide either the Python or the R code that
#  connects to the database and creates all necessary tables as per the 3NF relational schema you designed 
#  Note: (you may use either uni_small or uni_large database, or create your own new database using pgAdmin) 
#  Note: (you should create more than one table).
#
#  Important:
#  In your CREATE TABLE statements you must provide data types AND
#  primary/foreign keys (as applicable to relate your schemas).
#
#  NOTE: All actions must be performed in your either Python or R code. No points if the database
#        tables are created manually in pgAdmin and not with either Python or R code.
#
#  Make sure your code has no errors. 
#  When grading, we will run your script and see all the appropriate tables are created in the database properly.
#
#
#	SCORING RUBRIC
#	**************
#	1 points: Necessary packages are imported. Necessary database connections are made.
#	4 points: All necessary tables for 3NF are defined. 
#	3 points: All tables has correct PK/FK as appropriate, and all columns has correct data type, 
#					relevant columns has appropriate NULL / NOT NULL constraints defined.
#	2 points: Simply copy/pasting and executing the script provided in answer-1 
#				creates all tables successfully in the database without any error
#				*1 minor (but only syntax) error will still get you 1 point
# ****************************************************************************************************************
# SOLUTION: Question 1

#
# ****************************************************************************************************************
import pandas as pd
from sqlalchemy import create_engine

conn_url = "postgresql://postgres:123456@localhost/postgres"

engine = create_engine(conn_url)

Connection = engine.connect()

create_company = """
    CREATE TABLE company (
      company_id int NOT NULL,
      company_name varchar(255) NOT NULL,
      CONSTRAINT "company_pkey" PRIMARY KEY (company_id)
    ) ;
"""

create_product = """
    CREATE TABLE product (
        product_id int NOT NULL,
        "Product_name" varchar(255) NOT NULL,
        "Product_model" varchar(255) NOT NULL,
        "TypeName" varchar(255) DEFAULT NULL,
        "Inches" double precision NOT NULL,
        "ScreenResolution" varchar(255) DEFAULT NULL,
        "Cpu" varchar(255) DEFAULT NULL,
        "Ram" varchar(255) DEFAULT NULL,
        "HDD"	varchar(255) DEFAULT NULL,
        "SSD"	varchar(255) DEFAULT NULL,
        "Flash_Storage" varchar(255) DEFAULT NULL,
        "Memory" varchar(255) DEFAULT NULL,
        "Gpu" varchar(255) DEFAULT NULL,
        "OpSys" varchar(255) DEFAULT NULL,
        "Weight" varchar(255) NOT NULL,
        "Price_euros" double precision NOT NULL,
        CONSTRAINT "product_pkey" PRIMARY KEY (product_id)
    );
"""

create_company_have_product = """
    CREATE TABLE company_have_product (
          company_id int NOT NULL,
          product_id int NOT NULL,
          CONSTRAINT "company_have_product_pkey" PRIMARY KEY (company_id,product_id),
          CONSTRAINT "company_have_product_fkey_company" FOREIGN KEY (company_id) REFERENCES company (company_id),
          CONSTRAINT "company_have_product_fkey_product" FOREIGN KEY (product_id) REFERENCES product (product_id)
    );
"""

# execute
Connection.execute(create_company)
Connection.execute(create_product)
Connection.execute(create_company_have_product)

# -----------------------------------------------------------------------------
#
#  QUESTION 2 (20 points)
#  ------------------------
#  Provide the either Python or R code that populates the database with the data from the
#  provided "HW6_DATA.csv" file. You can download the dataset
#  from the assignment page. It is anticipated that you will perform several steps
#  of data processing in Python or R in order to extract, transform and load all data from
#  the file to the database tables. Manual transformations in a spreadsheet, or
#  similar, are not acceptable, all work must be done in either Python or R. Make sure your code
#  has no errors, no partial credit for code that returns errors. When grading,
#  we will run your script and see all the appropriate data is inserted in correct tables in the database.
#
#	SCORING RUBRIC
#	**************
#	2 points: Necessary packages are imported. Necessary database connections are made.
#	1 points: Code is reading / loading correct data file without any error. 
#	8 points: All 3NF tables are loaded correctly with correct data.
#	4 points: Brand and Model in Product are split into individual columns and are transformed correctly 
#			  to load in appropriate 3NF table relevant columns has appropriate NULL / NOT NULL constraints defined.
#	2 points: Apropriate PK values are generated and loaded into tables correctly
#	3 points: Simply copy/pasting and executing the script provided in answer-2 
#				loads all the data successfully in the appropriate tables without any error
#			  *1 minor (but only syntax) error will still get you 1 point


# ****************************************************************************************************************
# SOLUTION: Question 2

# load  the data
data = pd.read_csv("laptop_price.csv", encoding='ISO-8859-1')

# Split Brand and Model
lst = ['Product']
name = []
model = []
for cName in lst:
    c = data[cName]
    for i in c:
        temp = i.split(" ")
        i_inverse = i[::-1]
        index = len(i) - i_inverse.index(' ')
        name.append(i[:index - 1])
        model.append(i[index:len(i)])
    column_name = data.columns.values.tolist()
    data.insert(loc=column_name.index(cName) + 1, column=cName + '_model', value=model)
    data.insert(loc=column_name.index(cName) + 1, column=cName + '_name', value=name)
    # data=data.drop(cName,axis=1)
    name.clear()
    model.clear()
# Split HDD , SSD, and Flash_Storage
HDD = []
SSD = []
Flash_Storage = []
c = data["Memory"]

for i in c:
    temp = i.split(" ")
    temp_set = {'HDD': '0', 'SSD': '0', 'Flash_Storage': '0'}
    for k in range(0, len(temp)):
        if temp[k] == 'HDD':
            temp_set['HDD'] = temp[k - 1]
        elif temp[k] == 'SSD':
            temp_set['SSD'] = temp[k - 1]
        elif temp[k] == 'Flash':
            temp_set['Flash_Storage'] = temp[k - 1]
        else:
            pass
    HDD.append(temp_set['HDD'])
    SSD.append(temp_set['SSD'])
    Flash_Storage.append(temp_set['Flash_Storage'])
column_name = data.columns.values.tolist()
data.insert(loc=column_name.index("Memory") + 1, column='Flash_Storage', value=Flash_Storage)
data.insert(loc=column_name.index("Memory") + 1, column='SSD', value=SSD)
data.insert(loc=column_name.index("Memory") + 1, column='HDD', value=HDD)
# Extract company table and save company ID and name with dictionary
company_set = {}
company = data[['Company']]
company = company.drop_duplicates(subset={'Company'})
company.index = list(range(len(company.index)))
company.insert(loc=0, column='company_id', value=list(range(len(company.index))))
for i in company.values:
    company_set[i[1]] = i[0]
company.rename(columns={'Company': 'company_name'}, inplace=True)
# Extract product table
product = data[['Company', 'Product', 'Product_name', 'Product_model',
                'TypeName', 'Inches', 'ScreenResolution', 'Cpu', 'Ram',
                'HDD', 'SSD', 'Flash_Storage', 'Memory', 'Gpu', 'OpSys', 'Weight',
                'Price_euros']]
product = product.drop_duplicates(subset={'Company', 'Product', 'Product_name', 'Product_model',
                                          'TypeName', 'Inches', 'ScreenResolution', 'Cpu', 'Ram',
                                          'HDD', 'SSD', 'Flash_Storage', 'Memory', 'Gpu', 'OpSys', 'Weight',
                                          'Price_euros'})
product.index = list(range(len(product.index)))
product.insert(loc=0, column='product_id', value=list(range(len(product.index))))

# Extract company_have_product table
product_company = product[['Company', 'product_id']]
product_company_ids = []
for i in product_company.values:
    product_company_ids.append(company_set[i[0]])
product_company.insert(loc=0, column='company_id', value=product_company_ids)
product_company = product_company.drop("Company", axis=1)
product = product.drop("Company", axis=1)
product = product.drop("Product", axis=1)
# Insert to Table company
company.to_sql("company", Connection, index=False, if_exists='append')
# Insert to Table product
product.to_sql("product", Connection, index=False, if_exists='append')
#  Insert to Table company
product_company.to_sql("company_have_product", Connection, index=False, if_exists='append')
# ****************************************************************************************************************
# -----------------------------------------------------------------------------
#
#  QUESTION 3 (5 points)
#  ------------------------------------------
#
#	To create data logic abstraction, create 3 views in the database such that 
#	your answers can use any number of select statements 
#	but can keep those statements as simple as selecting from a view (select * from <your view name>)
#
# ****************************************************************************************************************
# SOLUTION: Question 3
# See provided solution
#
create_view_for_company = """
    Create view company_view as SELECT * from company; 
"""
create_view_for_product = """
    Create view product_view as SELECT * from product;
"""
create_view_for_company_have_product = """
    Create view company_have_product_view as SELECT * from product;
"""

Connection.execute(create_view_for_company)
Connection.execute(create_view_for_product)
Connection.execute(create_view_for_company_have_product)

# Test for select from a view
select_view_company = """
    SELECT * from company_view;
"""
result = Connection.execute(select_view_company)
for i in result:
    print(i)
# ****************************************************************************************************************
# -----------------------------------------------------------------------------
#
#  QUESTION 4 (5 points)
#  ----------------------------------------------------
#
#  Write the Python or R code that creates a view and queries the database to displays the average 
#  laptop price (euros) per company
#  (select * from <your view name>)
#
# ****************************************************************************************************************
# SOLUTION: Question 4
# See provided solution
#
# ****************************************************************************************************************
# -----------------------------------------------------------------------------
create_view_for_AVG_price_of_company = """
    Create view view_for_AVG_price_of_company as
    SELECT company_name,AVG(product."Price_euros") from 
    company INNER JOIN company_have_product 
    on company.company_id=company_have_product.company_id 
    INNER JOIN product
    on product.product_id=company_have_product.product_id
    GROUP BY company_name;
"""
select_view_for_AVG_price_of_company = """
    SELECT * from view_for_AVG_price_of_company;
"""
Connection.execute(create_view_for_AVG_price_of_company)
result = Connection.execute(select_view_company)
for i in result:
    print(i)
#  QUESTION 5 (5 points)
#  -----------------------------------------------------
#
#  Provide either a Python or R code such that if the whole script is executed 
#  once, then ALL the views and tables created by you for this exercise will be deleted in one go wihtout generating any error.
#	Hint: A few objects you have created would have dependencies on other objects you have created
#
#	SCORING RUBRIC
#	**************
#	2 points: Sequence of deletion is correct.
#	3 points: Simply copy/pasting and executing the script provided in answer-6 
#				deletes ALL the database objects created in all the above answeres, without any error
#			*even a minor (including syntax) error will not get you this 1 point
# ****************************************************************************************************************
# SOLUTION: Question 6
DROP_ALL = """
    ALTER TABLE company_have_product DROP CONSTRAINT company_have_product_fkey_company;
    ALTER TABLE company_have_product DROP CONSTRAINT company_have_product_fkey_product;

    drop view company_view;
    drop view view_for_avg_price_of_company;
    drop view product_view;
    drop view company_have_product_view;
    drop table company_have_product;
    drop table product;
    drop table company;
"""
Connection.execute(DROP_ALL)

# See provided solution
#
# ****************************************************************************************************************

# -----------------------------------------------------------------------------
