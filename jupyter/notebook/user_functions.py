from pyspark.sql.functions import * 
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import *
import logging
import json
import os
import shutil


def display(df:DataFrame):
    """
    Displays the contents of a PySpark DataFrame in a tabular format.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame to be displayed.
    """
    df.show(50,truncate=False)


def col_rename_with_mapping(df:DataFrame,col_mapping_path:str):
    """
    Renames the columns of the dataframe with the provided mapping json

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame.
        col_mapping_path (str): The path to the JSON file containing the column mapping.

    Returns:
        pyspark.sql.DataFrame: The PySpark DataFrame with renamed columns
    """
    
    logging.info("Checking if the input is Pyspark DataFrame or not")
    if isinstance(df, DataFrame):
        logging.info("Input is a Spark DataFrame. Proceeding with column renaming.")
        logging.info(f"Loading column mapping from path : {col_mapping_path}")
        with open(col_mapping_path, 'r') as f:
            col_mapping = json.load(f)
        logging.info("Column mapping loaded successfully , Proceeding with column renaming")
        df = df.select([col(c).alias(col_mapping.get(c, c)) for c in df.columns])
        logging.info("Columns rename completed as per mapping")
        return df
    else:
        logging.error("Input is not a Spark DataFrame, Please provide supported DataFrame input")
        raise TypeError("Input must be a Spark DataFrame, Please provide supported DataFrame input")


def remove_special_characters(df,column_name):
    """
    Remove special characters from a column in a PySpark DataFrame.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame.
        column_name (str): The name of the column to remove special characters from.

    Returns:
        pyspark.sql.DataFrame: The PySpark DataFrame with the column's special characters removed.
    """
    logging.debug(f"Removing special characters from column: {column_name}")
    df = df.withColumn(column_name,regexp_replace(col(column_name),"[^a-zA-Z0-9, -]",""))
    logging.debug(f"Special characters removed from column: {column_name}")
    return df

def convert_to_numeric(df,column_name,to_double = False):
    """
    Converts the specified column in a PySpark DataFrame to numeric type.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame.
        column_name (str): The name of the column to convert to numeric type.

    Returns:
        pyspark.sql.DataFrame: The PySpark DataFrame with the column's data type converted to numeric.
    """
    numeric_type = "double" if to_double else "int"
    logging.debug(f"Converting column '{column_name}' to {numeric_type}")
    if to_double:
        df = df.withColumn(column_name,(regexp_replace(col(column_name),"[^0-9.]","").cast("double")))\
               .withColumn(column_name,round(col(column_name),2))
        logging.debug(f"Column '{column_name}' converted to double with 2 decimal places")
    else:
        df = df.withColumn(column_name,regexp_replace(col(column_name),"[^0-9]","").cast("int"))
        logging.debug(f"Column '{column_name}' converted to int")
    return df


def convert_to_datetime(df,column_name):
    """
    Converts the specified column in a PySpark DataFrame to datetime type.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame.
        column_name (str): The name of the column to convert to datetime type.

    Returns:
        pyspark.sql.DataFrame: The PySpark DataFrame with the column's data type converted to datetime.
    """
    logging.debug(f"Converting column '{column_name}' to datetime")
    df = df.withColumn(column_name, to_timestamp(col(column_name), "yyyy-MM-dd'T'HH:mm:ss.SSS")) 
    logging.debug(f"Column '{column_name}' converted to datetime format")
    return df

def convert_to_tilecase(df,column_name):
    """
    Converts the specified column data in a PySpark DataFrame to Title Case.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame.
        column_name (str): The name of the column whose data needs to be converted to Title Case.

    Returns:
        pyspark.sql.DataFrame: The PySpark DataFrame with the column's data converted to Title Case.
    """
    logging.debug(f"Converting column '{column_name}' to Title Case")
    df = df.withColumn(column_name,initcap(trim(col(column_name))))
    logging.debug(f"Column '{column_name}' converted to Title Case")
    return df


def remove_duplicates(df,dedup_grain:list,order_grain:list,is_desc:bool):
    """
    Removes the duplicates from the dataframe based on the provided grain

    Args:
       df (pyspark.sql.DataFrame): The PySpark DataFrame.
       dedup_grain (list): A list of column names to partition the data by.
       order_grain (list): A list of column names to order the data by.
       is_desc (bool): Wheather we want to order the data by Descending order or Acsending Order

    Returns:
        pyspark.sql.DataFrame: The PySpark DataFrame without any duplicates on the provided grain

    """
    logging.info(f"Removing duplicates with dedup_grain={dedup_grain}, order_grain={order_grain}, is_desc={is_desc}")
    original_count = df.count()
    if is_desc :
        order_grain = [col(c).desc() for c in order_grain]
    else:
        order_grain = [col(c) for c in order_grain]

    win_spec = Window.partitionBy(dedup_grain).orderBy(order_grain)

    df_dedup = df.withColumn("rank",row_number().over(win_spec))\
                 .filter('rank = 1')\
                 .drop('rank')
    dedup_count = df_dedup.count()
    logging.info(f"Deduplication complete: {original_count} rows reduced to {dedup_count} rows")
    return df_dedup


def annualize_salary(df):
    """
    Creates annualized salary columns in the provided PySpark DataFrame based on the salary frequency.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame containing salary information.

    Returns:
        pyspark.sql.DataFrame: The PySpark DataFrame with annualized salary columns added.
    """
    logging.info("Starting salary annualization")
    df = df.withColumn("avg_salary", round((col("salary_min_range") + col("salary_max_range")) / 2 , 2))

    df = df\
        .withColumn("annualized_salary_min_range",
            when(col("salary_frequency") == "Annual", col("salary_min_range"))
            .when(col("salary_frequency") == "Hourly", col("salary_min_range") * 2080)
            .when(col("salary_frequency") == "Daily", col("salary_min_range") * 260)
            .otherwise(col("salary_min_range"))
        )\
        .withColumn("annualized_salary_max_range",
            when(col("salary_frequency") == "Annual", col("salary_max_range"))
            .when(col("salary_frequency") == "Hourly", col("salary_max_range") * 2080)
            .when(col("salary_frequency") == "Daily", col("salary_max_range") * 260)
            .otherwise(col("salary_max_range"))
        )\
        .withColumn("annualized_salary_avg_range",
            when(col("salary_frequency") == "Annual", col("avg_salary"))
            .when(col("salary_frequency") == "Hourly", col("avg_salary") * 2080)
            .when(col("salary_frequency") == "Daily", col("avg_salary") * 260)
            .otherwise(col("avg_salary"))
        )\

    logging.info("Salary annualization completed")
    return df

def create_qualification_indicator(df):
    """
    Creates a qualification indicator column in the provided PySpark DataFrame based on the minimum qualification requirements.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame containing minimum qualification requirements.
    Returns:
        pyspark.sql.DataFrame: The PySpark DataFrame with a new column 'is_degree_req' indicating whether a degree is required based on the minimum qualification requirements.
    """
    logging.info("Creating qualification indicator column")
    df = df.withColumn("is_degree_req", 
        when(lower(col("min_qualify_requirements")).rlike("master|phd|doctorate|graduate|degree|graduation"), lit(1)).otherwise(lit(0)))
    logging.info("Qualification indicator column created")
    return df

def drop_columns(df,columns_to_drop):
    """
    Drops the specified columns from the provided PySpark DataFrame.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame from which columns need to be dropped.
        columns_to_drop (list): A list of column names that need to be dropped from the DataFrame.

    Returns:
        pyspark.sql.DataFrame: The PySpark DataFrame with the specified columns dropped.
    """
    logging.info(f"Dropping {len(columns_to_drop)} columns: {columns_to_drop}")
    df = df.drop(*columns_to_drop)
    logging.debug(f"Successfully dropped columns")
    return df


def export_to_csv(df,output_path,file_name):
    """
    Exports the provided PySpark DataFrame to a CSV file at the specified output path.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame to be exported.
        output_path (str): The file path where the CSV file will be saved.
        file_name (str): The name of the CSV file to be saved.

    Returns:
        None: This function does not return anything. It saves the DataFrame as a CSV file at the specified location.
    """
    logging.info(f"Starting CSV export: {output_path}/{file_name}")
    logging.info(f"Number of rows to export: {df.count()}")
    
    temp_output_dir = output_path + "/temp_output_folder"
    final_filename = output_path + "/" + file_name

    logging.debug(f"Writing to temporary directory: {temp_output_dir}")
    df.coalesce(1).write.option("header", "true").csv(temp_output_dir, mode="overwrite")
    logging.debug("Data written to temporary directory successfully")

    files = os.listdir(temp_output_dir)
    for file in files:
        if file.endswith(".csv"):
            logging.debug(f"Moving {file} to {final_filename}")
            os.rename(os.path.join(temp_output_dir, file), final_filename)
            break

    logging.debug(f"Removing temporary directory: {temp_output_dir}")
    shutil.rmtree(temp_output_dir)
    logging.info(f"CSV export completed: {final_filename}")
