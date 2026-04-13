import csv
from src.db.dynamodb_export import exportFromDynamoDB

def generate_daily_mis(timestamp, database):
    date, month, year = timestamp[0], timestamp[1], timestamp[2]
    
    csv_str, db_data = exportFromDynamoDB(
        database, "GSI-PK", f"{year}{month}", "GSI-SK", date
    )

    return csv_str