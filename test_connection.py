import snowflake.connector
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Create connection
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)

print("✅ Connected to Snowflake successfully!")

# Test query
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM SUPPLIERS")
result = cursor.fetchone()
print(f"📊 Number of suppliers in database: {result[0]}")

cursor.close()
conn.close()