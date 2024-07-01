from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, when, split, length, lit, collect_list, monotonically_increasing_id, concat_ws, max
import pandas as pd
import shutil, os

# Initialize Spark session
spark = SparkSession.builder.appName("ZomatoDataPipeline").getOrCreate()

# Config Variables
source_directory = "files/"
processed_directory = "processed_files/"
location_lookup = "lookup/Areas_in_blore.xlsx"
output_directory = "output/"

# Path to the directory where processed files are tracked
processed_files_path = "processed_files.txt"


# List all files from source directory
file_list = os.listdir(source_directory)
print(f"Files in \"{source_directory}\" directory: {file_list}")

# Read Look-up of Area Location
df = pd.read_excel(location_lookup)

# Display the DataFrame
# print(df)

area_lookup = df['Area'].tolist()
print(f"area_lookup = {area_lookup}, type = {type(area_lookup)}")

# Function to check if the file has been processed
def is_new_file(file_name, processed_files_path):
    if not os.path.exists(processed_files_path):
        return True
    with open(processed_files_path, 'r') as file:
        processed_files = file.read().splitlines()
    return file_name not in processed_files

# Function to check if the file is empty
def is_file_empty(file_path):
    return os.path.getsize(file_path) == 0

# Function to check if the file is a CSV
def is_csv_file(file_path):
    return file_path.lower().endswith('.csv')

# Function to validate phone numbers
def validate_phone(phone):
    phone = phone.replace("+", "").replace(" ", "")
    return phone if len(phone) == 10 and phone.isdigit() else None

# Function to mark a file as processed
def mark_as_processed(file_name, processed_files_path):
    with open(processed_files_path, 'a') as file:
        file.write(file_name + "\n")

def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Directory '{directory}' created successfully.")
    else:
        print(f"Directory '{directory}' already exists.")

# Function to move file
def move_file(source):
    file = source.replace(f"{source_directory}", "")
    destination = f"{processed_directory}{file}"
    try:
        print(file + " moved to " + shutil.move(source, destination) + "\n\n")
    except Exception as e:
        print(f"Exception: {e}")

# Process each file in the list
for file_name in file_list:
    try:
        print(f"\n\nProcessing file: {file_name}")
        file_name = f"{source_directory}{file_name}"

        # 1.1 Is this a new file?
        if not is_new_file(file_name, processed_files_path):
            print(f"Skipping {file_name}, already processed.")
            continue

        # 1.2 Is the file empty?
        if is_file_empty(file_name):
            print(f"Skipping {file_name}, file is empty.")
            continue

        # 1.3 Is the file extension .csv?
        if not is_csv_file(file_name):
            print(f"Skipping {file_name}, not a CSV file.")
            continue

        # Read the CSV file
        df = spark.read.csv(file_name, header=True)

        # Add row number to the DataFrame
        df = df.withColumn("row_num", monotonically_increasing_id())

        # Process the DataFrame (example transformation)
        # df.show()
        # df.printSchema()
        print(f"count for {file_name}: {df.count()}")

        # 2.1.a To remove "+" and blank(\s)
        df = df.withColumn("phone",
                           when(col("phone").isNotNull(),
                                regexp_replace(col("phone"), r"^\+|\s+", ""))  # Remove leading + and spaces
                           .otherwise(None))  # Set to None if null or invalid

        # 2.1.b Validate correct phone number format
        df = df.withColumn("phone",
                           when(col("phone").rlike(r"^\d{10,}$"), col("phone"))
                           .otherwise(None))
        # df.select('phone').show()

        # 2.3 Descriptive fields like address, reviews_list can be cleaned by removing special characters or junk characters, etc.
        # df.select(df['address']).show()
        df = df.withColumn("address_clean", regexp_replace(col("address"), "[^a-zA-Z0-9\\s]", "")) \
            .withColumn("address_clean", regexp_replace(col("address"), "\n", ""))
        # df.select(df['address_clean']).show()

        df = df.withColumn("reviews_list", regexp_replace(col("reviews_list"), r"[\[\]]", "")) \
            .withColumn("review_1", split(col("reviews_list"), ",").getItem(0)) \
            .withColumn("review_2", split(col("reviews_list"), ",").getItem(1)) \
            .withColumn("review_1", regexp_replace(col("review_1"), r'[\"()\']', '')) \
            .withColumn("review_2", regexp_replace(col("review_2"), r'["\\n\']+', ''))
        # df.select('reviews_list', 'review_1', 'review_2').show()

        # 2.3.a The field data can be split and stored in two separate fields
        df = df.withColumn("dish_liked", regexp_replace(col("dish_liked"), "\n", "")) \
            .withColumn("dish_1", split(col("dish_liked"), ",").getItem(0)) \
            .withColumn("dish_2", split(col("dish_liked"), ",").getItem(1))
        # df.select('dish_liked', 'dish_1', 'dish_2').show()

        df = df.withColumn("cuisines", regexp_replace(col("cuisines"), "\n", "")) \
            .withColumn("cuisine_1", split(col("cuisines"), ",").getItem(0)) \
            .withColumn("cuisine_2", split(col("cuisines"), ",").getItem(1))
        # df.select('cuisines', 'cuisine_1', 'cuisine_2').show()

        # 3 - Custom Data Quality Check Module (for name)
        # Format check: names should not contain numeric characters
        # Length check: Name length should be at least 2 characters
        # df.select('name').show()
        df = df.withColumn("name", when(col("name").rlike(r"\d"), None).otherwise(col("name"))) \
              .withColumn("name", when(length(col("name")) > 2, col("name")).otherwise(None))
        # df.select('name').show()

        # 4. Location Validation Module
        df = df.withColumn('valid_location', when(col('location').isin(area_lookup), True).otherwise(False))
        # df.select('valid_location').show()
        not_valid_location_df = df.filter(~col("valid_location"))

        # 2.2 Check for null values in all fields and separate into reject_df
        reject_df = df.filter(
            col("name").isNull() |
            col("phone").isNull() |
            col("location").isNull()
        )

        df = df.filter(
            ~(col("name").isNull() |
            col("phone").isNull() |
            col("location").isNull())
        )

        # Create reject_metadata DataFrame
        reject_metadata = reject_df.select(
            lit("null").alias("Type_of_issue"),
            concat_ws(",", collect_list("row_num")).alias("Row_num_list")  # Flatten array to comma-separated string
        ).groupBy("Type_of_issue").agg(
            max("Row_num_list").alias("Row_num_list")  # Use max to convert to string (workaround)
        )
        # Show the reject_metadata
        # reject_metadata.show(truncate=False)

        # List of columns to exclude
        columns_to_exclude = ["address", "reviews_list", "dish_liked", "cuisines"]

        # Create new_df by selecting columns not in columns_to_exclude
        new_df = df.select([col for col in df.columns if col not in columns_to_exclude])

        # Creating pandas df
        pd_df = new_df.toPandas()
        pd_not_valid_location_df = not_valid_location_df.toPandas()
        pd_reject_df = reject_df.toPandas()
        pd_reject_metadata = reject_metadata.toPandas()

        # Create the directory if it does not exist
        create_directory(output_directory)
        create_directory(f"{output_directory}{file_name}")

        # Creating output files
        pd_df.to_csv(f'{output_directory}{file_name}/a.out', index=False)
        print(f'{output_directory}{file_name}/a.out created successfully.')

        pd_not_valid_location_df.to_csv(f'{output_directory}{file_name}/not_valid_location.bad', index=False)
        print(f'{output_directory}{file_name}/not_valid_location.bad created successfully.')

        pd_reject_df.to_csv(f'{output_directory}{file_name}/b.bad', index=False)
        print(f'{output_directory}{file_name}/b.bad created successfully.')

        pd_reject_metadata.to_csv(f'{output_directory}{file_name}/b.bad_metadata.csv', index=False)
        print(f'{output_directory}{file_name}/b.bad_metadata.csv created successfully.')

        print(f"       df count = {df.count()}")
        print(f"reject df count = {reject_df.count()}")
        print(f"          Total = {df.count() + reject_df.count()}")

        # Mark the file as processed
        mark_as_processed(file_name, processed_files_path)

        print(f"Processing file: {file_name} Completed...")

    except Exception as e:
        print(f"{file_name} Failed...\n\nWith ERROR: {e}")
    finally:
        # Move file
        move_file(file_name)

# Stop the SparkSession
spark.stop()