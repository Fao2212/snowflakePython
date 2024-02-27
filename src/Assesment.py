from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
from snowflake.snowpark.functions import col, concat, lit
from utils import snowpark_utils

def getTable(session,tableName):
    return session.table(tableName)

def main():

    print("Creating session")
    session = snowpark_utils.get_snowpark_session()
    session.use_schema("PUBLIC")
    originalDataFrame = getTable(session,"ORIGINAL_DATA")
    print(originalDataFrame.count())

    #Remove duplicate transactions.
    originalDataFrame = originalDataFrame.drop_duplicates("TRANSACTION_ID")
    print(originalDataFrame.count())

    #Create client table and remove duplicates
    df_client = originalDataFrame.drop_duplicates("CLIENT_ID")
    df_client = df_client.select(col("CLIENT_ID"),concat(lit("Client_"),col("CLIENT_ID")).alias("CLIENT_NAME"),col("CLIENT_LASTNAME"),col("EMAIL"))
    df_client.write.save_as_table("CLIENT", mode="overwrite", table_type="transient")
    clientTable = session.table("CLIENT")
    clientTable.show()

    #Create store table and remove duplicates
    df_store = originalDataFrame.drop_duplicates("STORE_ID")
    df_store = df_store.select(col("STORE_ID"),concat(lit("Store_"),col("STORE_ID")).alias("STORE_NAME"),col("LOCATION"))
    df_store.write.save_as_table("STORE", mode="overwrite", table_type="transient")
    storeTable = session.table("STORE")
    storeTable.show()

    #Create product table and remove duplicates
    df_product = originalDataFrame.drop_duplicates("PRODUCT_ID")
    df_product = df_product.select(col("PRODUCT_ID"),concat(lit("Product_"),col("PRODUCT_ID")).alias("PRODUCT_NAME"),col("CATEGORY"),col("BRAND"))
    df_product.write.save_as_table("PRODUCT", mode="overwrite", table_type="transient")
    productTable = session.table("PRODUCT")
    productTable.show()

    #Create address table and remove duplicates
    df_adress = originalDataFrame.drop_duplicates("ADDRESS_ID")
    df_adress = df_adress.select(col("ADDRESS_ID"),col("STREET"),col("CITY"),col("STATE"),col("ZIP_CODE"))
    df_adress.write.save_as_table("ADDRESS", mode="overwrite", table_type="transient")
    addressTable = session.table("ADDRESS")
    addressTable.show()

    #Create FACT TABLE
    df_sales = originalDataFrame.select(col("TRANSACTION_ID"),col("QUANTITY_OF_ITEMS_SOLD"),col("UNIT_PRICE"),col("DISCOUNT"),col("CLIENT_ID")
                                        ,col("STORE_ID"),col("PRODUCT_ID"),col("ADDRESS_ID"))
    df_sales.write.save_as_table("SALES", mode="overwrite", table_type="transient")
    salesTable = session.table("SALES")
    salesTable.show()

    session.close()
    print("Session closed")

main()