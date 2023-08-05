import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# Script generated for node Custom_Transform_time
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF()


    # Add the required date attributes to the DataFrame
    df = (
        df.withColumn("TIME_ID", df["T_DATE"])
        .withColumn("DAY_OF_MONTH", F.dayofmonth(df["T_DATE"]))
        .withColumn("DAY_OF_WEEK", F.date_format(df["T_DATE"], "E"))
        .withColumn("MONTH", F.month(df["T_DATE"]))
        .withColumn("QUARTER", F.quarter(df["T_DATE"]))
        .withColumn("YEAR", F.year(df["T_DATE"]))
        .withColumn("Week_no", F.weekofyear(df["T_DATE"]))
        .withColumn("Mid", F.dayofyear(df["T_DATE"]))
    )

    # Convert the DataFrame back to DynamicFrame and return the collection
    transformed_dfc = DynamicFrame.fromDF(df, glueContext, "transformed")
    return DynamicFrameCollection({"customTransform0": transformed_dfc}, glueContext)




args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node MASTERDATA
MASTERDATA_node1691017219822 = glueContext.create_dynamic_frame.from_catalog(
    database="retail_analytics",
    table_name="retail_masterdata",
    transformation_ctx="MASTERDATA_node1691017219822",
)

# Script generated for node TRANSACTION
TRANSACTION_node1691093661846 = glueContext.create_dynamic_frame.from_catalog(
    database="retail_analytics",
    table_name="retail_transactions",
    transformation_ctx="TRANSACTION_node1691093661846",
)

# Script generated for node product_tb
product_tb_node1691070359143 = DropFields.apply(
    frame=MASTERDATA_node1691017219822,
    paths=["PRICE", "SUPPLIER_ID", "SUPPLIER_NAME"],
    transformation_ctx="product_tb_node1691070359143",
)

# Script generated for node supplier_tb
supplier_tb_node1691091710129 = DropFields.apply(
    frame=MASTERDATA_node1691017219822,
    paths=["PRICE", "PRODUCT_ID", "PRODUCT_NAME"],
    transformation_ctx="supplier_tb_node1691091710129",
)

# Script generated for node customer_tb
customer_tb_node1691093705194 = DropFields.apply(
    frame=TRANSACTION_node1691093661846,
    paths=[
        "STORE_NAME",
        "QUANTITY",
        "T_DATE",
        "STORE_ID",
        "PRODUCT_ID",
        "TRANSACTION_ID",
    ],
    transformation_ctx="customer_tb_node1691093705194",
)

# Script generated for node store_tb
store_tb_node1691093913144 = DropFields.apply(
    frame=TRANSACTION_node1691093661846,
    paths=[
        "CUSTOMER_NAME",
        "QUANTITY",
        "CUSTOMER_ID",
        "T_DATE",
        "PRODUCT_ID",
        "TRANSACTION_ID",
    ],
    transformation_ctx="store_tb_node1691093913144",
)

# Script generated for node time_tb
time_tb_node1691094144809 = DropFields.apply(
    frame=TRANSACTION_node1691093661846,
    paths=[
        "CUSTOMER_NAME",
        "STORE_NAME",
        "QUANTITY",
        "CUSTOMER_ID",
        "STORE_ID",
        "PRODUCT_ID",
        "TRANSACTION_ID",
    ],
    transformation_ctx="time_tb_node1691094144809",
)

# Script generated for node Drop_Duplicates_supplier
Drop_Duplicates_supplier_node1691093010622 = DynamicFrame.fromDF(
    supplier_tb_node1691091710129.toDF().dropDuplicates(["SUPPLIER_ID"]),
    glueContext,
    "Drop_Duplicates_supplier_node1691093010622",
)

# Script generated for node Drop_Duplicates_Customer
Drop_Duplicates_Customer_node1691093709451 = DynamicFrame.fromDF(
    customer_tb_node1691093705194.toDF().dropDuplicates(["CUSTOMER_ID"]),
    glueContext,
    "Drop_Duplicates_Customer_node1691093709451",
)

# Script generated for node Drop_Duplicates_store
Drop_Duplicates_store_node1691093925909 = DynamicFrame.fromDF(
    store_tb_node1691093913144.toDF().dropDuplicates(["STORE_ID"]),
    glueContext,
    "Drop_Duplicates_store_node1691093925909",
)

# Script generated for node Drop_Duplicates_time
Drop_Duplicates_time_node1691094171238 = DynamicFrame.fromDF(
    time_tb_node1691094144809.toDF().dropDuplicates(["T_DATE"]),
    glueContext,
    "Drop_Duplicates_time_node1691094171238",
)

# Script generated for node Custom_Transform_time
Custom_Transform_time_node1691094312601 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {
            "Drop_Duplicates_time_node1691094171238": Drop_Duplicates_time_node1691094171238
        },
        glueContext,
    ),
)



specific_dynamic_frame = Custom_Transform_time_node1691094312601.select(list(Custom_Transform_time_node1691094312601.keys())[0])

# Select only the specified columns in the specific DynamicFrame
columns_to_select = ["TIME_ID", "DAY_OF_MONTH", "DAY_OF_WEEK", "MONTH", "QUARTER", "YEAR", "Week_no", "Mid"]
Select_From_Collection_time_node1691095954700 = specific_dynamic_frame.select_fields(columns_to_select)



# Script generated for node dw_product
dw_product_node1691017318798 = glueContext.write_dynamic_frame.from_catalog(
    frame=product_tb_node1691070359143,
    database="retail_analytics",
    table_name="retail_product",
    transformation_ctx="dw_product_node1691017318798",
)

# Script generated for node dw_supplier
dw_supplier_node1691091779372 = glueContext.write_dynamic_frame.from_catalog(
    frame=Drop_Duplicates_supplier_node1691093010622,
    database="retail_analytics",
    table_name="retail_supplier",
    transformation_ctx="dw_supplier_node1691091779372",
)

# Script generated for node dw_customer
dw_customer_node1691093718504 = glueContext.write_dynamic_frame.from_catalog(
    frame=Drop_Duplicates_Customer_node1691093709451,
    database="retail_analytics",
    table_name="retail_customer",
    transformation_ctx="dw_customer_node1691093718504",
)

# Script generated for node dw_store
dw_store_node1691093937088 = glueContext.write_dynamic_frame.from_catalog(
    frame=Drop_Duplicates_store_node1691093925909,
    database="retail_analytics",
    table_name="retail_store",
    transformation_ctx="dw_store_node1691093937088",
)

# Script generated for node dw_time
dw_time_node1691094179704 = glueContext.write_dynamic_frame.from_catalog(
    frame=Select_From_Collection_time_node1691095954700,
    database="retail_analytics",
    table_name="retail_time",
    transformation_ctx="dw_time_node1691094179704",
)




#  insert data in fact table 



# Read data from the transaction table
transaction = glueContext.create_dynamic_frame.from_catalog(
    database="retail_analytics",
    table_name="retail_transactions",
    transformation_ctx='transaction'
)

# Read data from the masterdata table
masterdata = glueContext.create_dynamic_frame.from_catalog(
 database="retail_analytics",
    table_name="retail_masterdata",
    transformation_ctx='masterdata'
)

# Read data from the dimension tables (Customer, Store, Product, Supplier, Time)
customer = glueContext.create_dynamic_frame.from_catalog(
   database="retail_analytics",
    table_name='retail_customer',
    transformation_ctx='customer'
)

store = glueContext.create_dynamic_frame.from_catalog(
  database="retail_analytics",
    table_name='retail_store',
    transformation_ctx='store'
)

product = glueContext.create_dynamic_frame.from_catalog(
    database="retail_analytics",
    table_name='retail_product',
    transformation_ctx='product'
)

supplier = glueContext.create_dynamic_frame.from_catalog(
    database="retail_analytics",
    table_name='retail_supplier',
    transformation_ctx='supplier'
)

time = glueContext.create_dynamic_frame.from_catalog(
  database="retail_analytics",
    table_name='retail_time',
    transformation_ctx='time'
)


# Join the transaction and masterdata tables based on PRODUCT_ID
joined_data = Join.apply(transaction, masterdata, 'PRODUCT_ID', 'PRODUCT_ID')

# Perform the JOINs on the dynamic frames to combine data
sales_joined = Join.apply(joined_data, customer, 'CUSTOMER_ID', 'CUSTOMER_ID')
sales_joined = Join.apply(sales_joined, store, 'STORE_ID', 'STORE_ID')
sales_joined = Join.apply(sales_joined, product, 'PRODUCT_ID', 'PRODUCT_ID')
sales_joined = Join.apply(sales_joined, supplier, 'SUPPLIER_ID', 'SUPPLIER_ID')
sales_joined = Join.apply(sales_joined, Select_From_Collection_time_node1691095954700, 'T_DATE', 'TIME_ID')


# Convert the DynamicFrame to DataFrame
df = sales_joined.toDF()

# Calculate the TOTAL_SALE based on QUANTITY and PRICE
df = df.withColumn('TOTAL_SALE', F.col('QUANTITY') * F.col('PRICE'))

# Convert DataFrame back to DynamicFrame
sales_joined = DynamicFrame.fromDF(df, glueContext, 'joined_data')





# Select the required columns for the Sales fact table
sales_selected = SelectFields.apply(sales_joined, 
    paths=['TRANSACTION_ID', 'CUSTOMER_ID', 'STORE_ID', 'PRODUCT_ID', 'SUPPLIER_ID', 'TIME_ID', 'QUANTITY', 'TOTAL_SALE']
)







# Write the DataFrame to the Sales fact table
dw_sales = glueContext.write_dynamic_frame.from_catalog(
    frame=sales_selected,
  database="retail_analytics",
    table_name='retail_sales',
    transformation_ctx='dw_sales'
)






job.commit()
