from pyspark.sql import SparkSession
from email_validator import validate_email, EmailNotValidError

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("LoadCSV") \
    .getOrCreate()

# Path to the CSV file
file_path = "data.csv"

# Load the CSV file into a DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Select specific columns: Name and Email
selected_columns_df = df.select("Name", "Email", "Phone4")
selected_columns_df = selected_columns_df.na.drop(
    subset=["Name", "Email", "Phone4"])

# Show the schema of the DataFrame
selected_columns_df.printSchema()

# Collect the DataFrame into a list of rows
rows = selected_columns_df.collect()

# Function to validate email


def validate_email_address(email):
    try:
        # Validate the email address
        validate_email(email)
        return "valid"
    except EmailNotValidError:
        return "invalid"


# Print the results
print(f"{'Name':<20} {'Email':<30} {'Status':<10}")
for row in rows:
    name = row['Name']
    email = row['Email']
    status = validate_email_address(email)
    print(f"{name:<20} {email:<30} {status:<10}")

# Stop the SparkSession
spark.stop()
