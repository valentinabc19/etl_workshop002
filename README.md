# etl_workshop002

**Developed by**  
* [Valentina Bueno Collazos](https://github.com/valentinabc19)  

---

## **Project Overview**  
This project automates the extraction, transformation and analysis of music data to identify patterns between:
- **Critical acclaim** (Grammy Awards)
- **Commercial popularity** (Spotify)
- **Community engagement** (Last.fm)

### **Key Features**  

**ETL**:  

The pipeline performs the following steps:

- Extract:

  - Reads raw Spotify data from a CSV file.
  - Extracts Grammy nomination data from a PostgreSQL database.
  - Makes requests to the API to extract the data.
    
- Transform:
  - Cleans and preprocesses Spotify, Grammy and the API datasets.
  - Merges the datasets to align Spotify artists with Grammy nominations and lastfm API metrics.

- Load:
  - Stores the final enriched dataset in a PostgreSQL database.
  - Uploads the results to Google Drive.

**Dashboard Visualizations**:  
- Pendiente!!:

### **Technologies Used**  
- Python 3.8+
- Apache Airflow
- PostgreSQL
- Google Drive API credentials (optional) 

---

## **Setup and Execution**  

### **1. Clone the Repository**  
```bash  
git clone [https://github.com/valentinabc19/etl_workshop002]
```  

### **2. Create a Virtual Environment**  
```bash  
python -m venv venv  
source venv/bin/activate  # Linux/Mac  
venv\Scripts\activate     # Windows  
```  

### **3. Configure Database Credentials**  
Create a `credentials.json` file in the project root:  
```json  
{  
    "db_host": "your_host",  
    "db_name": "your_db",  
    "db_user": "your_user",  
    "db_password": "your_password",  
    "db_port": "5432"  
}  
```
Ensure this file is included in .gitignore.

### **4. Install Dependencies**  
```bash  
pip install -r requirements.txt  
```  

### **5. Configure Airflow**  
```bash  
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow webserver --port 8080
airflow scheduler
```   

---

## **Prepare the data**  

Place the raw Spotify dataset (`spotify_dataset.csv`) in `data/raw`.
Ensure the Grammy nominations data is stored in your PostgreSQL database under the table `grammy_raw_data`. 

---

## Usage Guide: Airflow ETL Pipeline

## Accessing Airflow UI
1. Open your browser and navigate to:  
   **`http://localhost:8080`**  
2. Use the default credentials given in execution time of `airflow standalone`

## Triggering the DAG
1. In the Airflow UI, locate the `etl_workshop002` DAG  
2. Toggle the **On/Off** switch to enable it  
3. Click the **"Trigger DAG"** button to start execution  

## Monitoring the Pipeline
- **Real-time tracking**: View task status in the Grid View  
- **Detailed logs**: Access execution logs under:  
  ```bash
  airflow/logs/etl__workshop002/
  ```
**Note**: Ensure PostgreSQL is running and accessible during execution.  
