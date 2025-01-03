import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Table, MetaData
import cx_Oracle
import os
import requests

# Configurations for Oracle DB
DB_USER = os.getenv('ORACLE_USER', 'your_username')
DB_PASS = os.getenv('ORACLE_PASS', 'your_password')
DB_HOST = os.getenv('ORACLE_HOST', 'localhost')
DB_PORT = os.getenv('ORACLE_PORT', '1521')
DB_SERVICE = os.getenv('ORACLE_SERVICE', 'xe')

# Database connection string
ORACLE_DB = f'oracle+cx_oracle://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/?service_name={DB_SERVICE}'

# Initialize Oracle connection
engine = create_engine(ORACLE_DB)

# Load the raw CSV datasets
hr_data_path = 'hr_data.csv'
salary_data_path = 'salary_data.csv'
pension_data_path = 'pension_data.csv'

hr_df = pd.read_csv(hr_data_path)
salary_df = pd.read_csv(salary_data_path)
pension_df = pd.read_csv(pension_data_path)

# Display initial data overview
print("Initial HR Data Overview:")
print(hr_df.info())
print("\nInitial Salary Data Overview:")
print(salary_df.info())
print("\nInitial Pension Data Overview:")
print(pension_df.info())

# ============================
# Fetch Current Exchange Rate
# ============================

def get_exchange_rate():
    try:
        response = requests.get('https://api.exchangerate-api.com/v4/latest/GBP')
        data = response.json()
        return data['rates'].get('USD', 1)
    except Exception as e:
        print(f"Error fetching exchange rate: {str(e)}")
        return 1

exchange_rate = get_exchange_rate()
print(f"Current GBP to USD exchange rate: {exchange_rate}")

# ============================
# Data Cleaning Process
# ============================

# 1. Handle Missing Values
hr_df.fillna({'email': 'unknown@example.com', 'phone_number': '000-000-0000'}, inplace=True)
salary_df.fillna({'salary': 0}, inplace=True)
pension_df.fillna({'pension_amount': 0}, inplace=True)

# 2. Standardize Date Formats
hr_df['join_date'] = pd.to_datetime(hr_df['join_date'], errors='coerce').dt.strftime('%Y-%m-%d')
salary_df['payment_date'] = pd.to_datetime(salary_df['payment_date'], errors='coerce').dt.strftime('%Y-%m-%d')
pension_df['payment_date'] = pd.to_datetime(pension_df['payment_date'], errors='coerce').dt.strftime('%Y-%m-%d')

# 3. Normalize Text Fields
hr_df['name'] = hr_df['name'].str.title()
hr_df['email'] = hr_df['email'].str.lower()

# 4. Remove Duplicates
hr_df.drop_duplicates(subset=['email'], keep='last', inplace=True)
salary_df.drop_duplicates(subset=['employee_id', 'payment_date'], keep='last', inplace=True)
pension_df.drop_duplicates(subset=['employee_id', 'payment_date'], keep='last', inplace=True)

# 5. Join Datasets
merged_df = hr_df.merge(salary_df, on='employee_id', how='left')
merged_df = merged_df.merge(pension_df, on='employee_id', how='left')

# 6. Convert Salary to USD
merged_df['salary_usd'] = merged_df['salary'] * exchange_rate

# 7. Handle Outliers in Salary and Pension Fields
q1_salary = merged_df['salary'].quantile(0.25)
q3_salary = merged_df['salary'].quantile(0.75)
iqr_salary = q3_salary - q1_salary
lower_bound_salary = q1_salary - 1.5 * iqr_salary
upper_bound_salary = q3_salary + 1.5 * iqr_salary

merged_df['salary'] = np.where((merged_df['salary'] < lower_bound_salary) | (merged_df['salary'] > upper_bound_salary), np.nan, merged_df['salary'])
merged_df['salary'].fillna(merged_df['salary'].median(), inplace=True)

# Display cleaned data overview
print("\nCleaned and Merged Data Overview:")
print(merged_df.info())

# ============================
# Insert Cleaned Data into Oracle DB
# ============================

try:
    with engine.connect() as connection:
        meta = MetaData(bind=engine)
        meta.reflect(bind=engine)

        target_table = Table('EMPLOYEES', meta, autoload_with=engine)
        
        merged_df.to_sql('EMPLOYEES', con=engine, if_exists='append', index=False)
        print("\nData successfully inserted into Oracle Database.")
except Exception as e:
    print(f"Error inserting data: {str(e)}")

# ============================
# Optional: Export Cleaned Data to CSV for Backup
# ============================
output_path = 'cleaned_merged_data.csv'
merged_df.to_csv(output_path, index=False)
print(f"Cleaned and merged data saved to {output_path}")
