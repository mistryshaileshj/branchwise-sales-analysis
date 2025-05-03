import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import concat_ws, upper
import boto3
import time
from pyspark.sql.functions import month, year
from pyspark.sql.functions import split, col, size
from pyspark.sql.functions import format_string
from pyspark.sql.functions import count, round, sum, avg
from pyspark.sql.functions import to_timestamp, to_date, date_format
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.functions import trim, when
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import input_file_name

import pg8000
import logging
import pandas as pd

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 👇 Set legacy date/time parser policy
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Setup logger manually
logger = glueContext.get_logger()
#logger.setLevel(logging.INFO)

def main():
    # Define S3 path of the CSV file
    s3_path = "s3://s3bucketname/bucketfolder"
    
    logger.info("started reading all csv files in bucket folder to dataframe")
    
    dfcsv = spark.read.format("csv") \
        .option("header", "true") \
        .load(f"{s3_path}/*.csv")
    
    df = readcsvfiles(dfcsv)

    basicexploratoryanalysis(df)
    summarystats_dataaggregation(df)
    datafilteration(df)
    timebasedanalysis(df)

    writetoredshiftdbfromstaging(df)
    
    #print(f"saved to table")
    
def readcsvfiles(dffiles):
    try:
        flcount = dffiles.count()
        print(f"file count : {flcount}")
        
        logger.info("adding csv file path as a column to the dataframe")
        
        df_with_filename = dffiles.withColumn("sourcepath", input_file_name())
        df_with_filename = df_with_filename.withColumn(
            "filename",
            split(col("sourcepath"), "/").getItem(size(split(col("sourcepath"), "/")) - 1)
            )
        #split(col("sourcepath"), "/").getItem(-1)  # Get last part after `/`
        df_with_filename.select("Invoice ID", "sourcepath", "filename").show(10, truncate=False)

        # Read CSV file into a DynamicFrame
        #dynframe = glueContext.create_dynamic_frame.from_options(
        #  format_options={"withHeader": True, "separator": ","},
        #  connection_type="s3",
        #  format="csv",
        #  connection_options={"paths": [s3_path]},
        #)
        
        df = df_with_filename
        
        # Show schema and sample data
        print(f"schema")
        df.printSchema()
        print(f"data length : {df.count()}")
        #print(f"S3 path : {s3_path}")
        
        rename_dict = {
            "Invoice ID" : "invoiceid",
            "Customer type" : "customertype",
            "Product line" : "productline",
            "Unit price" : "unitprice",
            "Tax 5%" : "tax5pc",
            "gross margin percentage" : "grossmarginperc",
            "gross income" : "grossincome",
            "date" : "invdate",
            "time" : "invtime"
        }

        logger.info("renaming columns")
        for old_name, new_name in rename_dict.items():
            df = df.withColumnRenamed(old_name, new_name)

        columns_to_cast = {
            "unitprice" : DoubleType(), 
            "tax5pc" : DoubleType(), 
            "sales" : DoubleType(), 
            "cogs" : DoubleType(), 
            "quantity" : IntegerType(),
            "grossmarginperc" : DoubleType(), 
            "grossincome" : DoubleType(), 
            "rating" : DoubleType()
        }
        
        logger.info("modifying datatype for columns as rrequired")
        # Apply casting
        for col_name, new_type in columns_to_cast.items():
            df = df.withColumn(col_name, col(col_name).cast(new_type))
            
        # Clean up invdate and invtime
        df = df.withColumn("invtime", trim(col("invtime")))
        
        df = df.withColumn("invtime", when(col("invtime") == "", None).otherwise(col("invtime")))
      
        df = df.withColumn(
            "invdate_parsed",
            when(
                col("invdate").cast(StringType()).isNotNull(),
                when(to_date(col("invdate"), "MM/dd/yyyy").isNotNull(), to_date(col("invdate"), "MM/dd/yyyy"))
                .otherwise(to_date(col("invdate"), "MM-dd-yyyy"))
            ).otherwise(col("invdate"))  # if it's already a date, keep as-is
        )

        df = df.drop("invdate").withColumnRenamed("invdate_parsed", "invdate")
   
        logger.info("modifying date and time columns into a valid format")
        # Make sure invtime has 2-digit hour
        df = df.withColumn(
            "invtime",
            regexp_replace("invtime", r"^(\d{1}):", r"0\1:")
        )
        df = df.withColumn("invtime", to_timestamp("invtime", "hh:mm:ss a"))
        
        #dfsel.show(50, truncate=False)
        df.show(10, truncate=False)

    except Exception as e:
        # If anything goes wrong, this will catch it
        print(f"Error occurred: {str(e)}")
    
    return df

def writetoredshiftdbfromstaging(spark_df):
    # Write to Redshift
    USERNAME               = 'abcd-user'  # USERNAME FOR CLUSTER
    PASSWORD               = 'rspassword'  # PASSWORD
    REDSHIFT_DATABASE_NAME = 'dbname'
    REDSHIFT_ROLE          = 'arn:aws:iam::1234567890:role/rsrole'
    HOST                   = "hostlocation"
    REDSHIFT_DRIVER        = "redshiftdrivertouse"
    REDSHIFT_JDBC_URL      = "redshiftjdbcurlwith_portnameanddbname"
    REDSHIFT_URL           = REDSHIFT_JDBC_URL + "?user=" + USERNAME + "&password=" + PASSWORD
    IAM_ROLE_ARN           = REDSHIFT_ROLE

    redshift_tmp_dir="s3://redshift-temp-folder/"  #temp S3 path for staging
    
    seldf = spark_df.select("invoiceid",	"branch",	"city",	"customertype",	"gender",	"productline",	"unitprice",	"quantity",	"tax5pc",	"sales",	"invdate",	"invtime",	"payment",	"cogs",	"grossmarginperc",	"grossincome",	"rating", "filename")
    cnt = seldf.count()
    print(f"temp path : {cnt}")
    
    try:
        logger.info("writing to stage table")
        
        seldf.write \
            .format(REDSHIFT_DRIVER) \
            .option("url", REDSHIFT_URL) \
            .option("dbtable", "test.branchsales_stage") \
            .option("tempdir", redshift_tmp_dir) \
            .option("tempformat", "csv") \
            .option("aws_iam_role", REDSHIFT_ROLE) \
            .mode("overwrite") \
            .save()
        print(f"Inserted to staging table")
        
        #insert, update data to main table from staging table using insert update statement
        conn = pg8000.connect(
            host=HOST,
            database=REDSHIFT_DATABASE_NAME,
            user=USERNAME,
            password=PASSWORD,
            port='5439'
            )
        
        print(f"Connected to redshift db")
        
        cursor = conn.cursor()
        
        cursor.execute("Truncate table test.branchsales")
        conn.commit()
        
        logger.info("inserting and updating main table from staging table")
        
        cursor.execute("BEGIN;")
        cursor.execute("""
            UPDATE test.branchsales
            SET invoiceid = s.invoiceid, branch = s.branch, city = s.city, customertype = s.customertype, gender = s.gender
            , productline = s.productline, unitprice = s.unitprice, quantity = s.quantity, tax5pc = s.tax5pc
            , sales = s.sales, invdate = cast(s.invdate as date), invtime = s.invtime, payment = s.payment, cogs = s.cogs
            , grossmarginperc = s.grossmarginperc, grossincome = s.grossincome, rating = s.rating, dsfilename = s.filename
            FROM test.branchsales_stage s
            WHERE test.branchsales.invoiceid = s.invoiceid;
        """)
    
        cursor.execute("""
            INSERT INTO test.branchsales (invoiceid,	branch,	city,	customertype,	gender, productline, unitprice, quantity, tax5pc, sales, invdate, invtime, payment, cogs, grossmarginperc, grossincome, rating, dsfilename)
            SELECT s.invoiceid,	s.branch,	s.city,	s.customertype,	s.gender,	s.productline,	s.unitprice,	s.quantity,	s.tax5pc,	s.sales,	cast(s.invdate as date),	s.invtime,	s.payment,	s.cogs,	s.grossmarginperc,	s.grossincome,	s.rating,	s.filename
            FROM test.branchsales_stage s
            LEFT JOIN test.branchsales t ON t.invoiceid = s.invoiceid
            WHERE t.invoiceid IS NULL;
        """)
    
        cursor.execute("END;")
        conn.commit()
        cursor.close()
        conn.close()
    
        print("Data successfully written to redshift using staging")
        
    except Exception as e:
        # If anything goes wrong, this will catch it
        print(f"Error occurred: {str(e)}")

def basicexploratoryanalysis(dtframe):
    print("print schema")
    dtframe.printSchema()
    
    print("print sample 10 rows")
    dtframe.show(10, truncate=False)

    print("print total row count of the dataset")
    cnt = dtframe.count()
    print(f"record count : {cnt}")

    print("create a dataframe with selected columns and print")
    dfsel = dtframe.select("invoiceid","branch","productline","unitprice","quantity", "invdate")
    dfsel.show(10, truncate=False)

def summarystats_dataaggregation(dtframe):
    print("describe numeric columns")
    # Describe numeric columns
    dtframe.describe(["unitprice", "quantity", "sales", "rating", "grossmarginperc", "grossincome"]).show()

    print("Total sales per city")
    # Total sales per city
    sales_by_city = dtframe.groupBy("City").sum("sales").withColumnRenamed("sum(sales)", "total_sales")
    sales_by_city = sales_by_city.withColumn("total_sales", round("total_sales", 2))
    """ sales_by_city = dtframe.groupBy("City") \
        .sum("sales") \
        .withColumnRenamed("sum(sales)", "total_sales") \
        .withColumn("total_sales", round("total_sales", 2)) """
    sales_by_city.show()
    sales_by_city.printSchema()

    print("Average rating per product line")
    # Average rating per product line
    avg_rating = dtframe.groupBy("productline").avg("rating").withColumnRenamed("avg(rating)", "average_rating")
    avg_rating = avg_rating.withColumn("average_rating", round("average_rating", 2))
    avg_rating.show()

def datafilteration(dtframe):
    print("Transactions where sales > 500")
    # Transactions where sales > 500
    dfsel = dtframe.select("invoiceid","branch","productline","unitprice","quantity", "invdate", "sales")
    high_sales = dfsel.filter(dtframe.sales > 500)
    high_sales.show()

    print("Filter by payment method")
    # Filter by payment method
    dfsel = dtframe.select("invoiceid","branch","productline","unitprice","quantity", "invdate", "Payment")
    credit_card_sales = dfsel.filter(dtframe.Payment == "Credit card")
    credit_card_sales.show()

def timebasedanalysis(dtframe):
    print("Sales by month")
    sales_by_month = dtframe.withColumn("mth", month(dtframe.invdate)) \
                    .groupBy("mth") \
                    .sum("sales") \
                    .withColumnRenamed("sum(sales)", "monthly_sales")
    sales_by_month = sales_by_month.withColumn("monthly_sales", round("monthly_sales", 2))
    sales_by_month.show()

    print("branch wise sales analysis")

    # sales_by_branch = dtframe.groupBy("City", "Branch").agg( \
    #     sum("sales").alias("total_sales"), \
    #     avg("grossincome").alias("avg_income") \
    # ).orderBy("total_sales", ascending=False).show()

    sales_by_branch = dtframe.groupBy("City", "Branch").agg( \
        sum("sales").alias("total_sales"), \
        avg("grossincome").alias("avg_income"))
    sales_by_branch = sales_by_branch.withColumn("total_sales", round("total_sales", 2))
    sales_by_branch = sales_by_branch.orderBy("total_sales", ascending=False)
    sales_by_branch = sales_by_branch.withColumn("total_sales", round("total_sales", 2))
    sales_by_branch.show()

if __name__ == '__main__':
    main()