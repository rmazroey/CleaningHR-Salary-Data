# CleaningHR-Salary-Data
This project demonstrates a data pipeline that processes HR, salary, and pension datasets, cleans the data, and inserts it into an Oracle Database. The code showcases handling missing values, normalizing data, managing outliers, and converting currency (GBP to USD).
---

## Key Features
- **Data Cleaning and Transformation**:  
  - Handles missing data for HR and financial records.  
  - Standardizes date formats.  
  - Normalizes text fields for consistency.  
  - Removes duplicate records.  

- **Exchange Rate API Integration**:  
  - Fetches the latest GBP to USD exchange rate via `https://exchangerate-api.com` and converts salary amounts to USD.  

- **Database Integration**:  
  - Inserts cleaned and merged data directly into Oracle Database using SQLAlchemy and cx_Oracle.  
  - Supports automatic schema reflection and dynamic table interaction.  

- **Multiple Dataset Join**:  
  - Combines HR, salary, and pension datasets based on employee IDs to create a comprehensive view.  

