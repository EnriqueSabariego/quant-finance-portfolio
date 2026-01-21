# Quant Finance Portfolio & Tableau Dashboard

This project demonstrates an end-to-end quantitative finance data pipeline built using **Databricks Community Edition** following a **Bronze / Silver / Gold architecture**, with final insights visualized in **Tableau Public**.

📊 **Dashboard**

Tableau Public link:  
https://public.tableau.com/app/profile/enrique.sabariego/viz/QuantFinanceTableauDashboard/Dashboard1?publish=yes

📊 **Dashboard Preview**

![Tableau Dashboard](images/Quant_Finance_Tableau_Dashboard_Screenshoot.png)

🗂 **Dataset**

Historical stock price data for selected equities, ETFs, and indices (CSV format).  
A sample CSV is included for reproducibility.

🏗 **Architecture**

- **Bronze layer:** Raw CSV ingestion into Delta tables  
- **Silver layer:** Data cleaning, feature engineering, returns, rolling volatility, drawdown  
- **Gold layer:** Financial KPIs and aggregations  

📈 **Gold Tables**

- Daily cumulative returns  
- Rolling volatility (20-day)  
- Drawdown analysis  
- KPI summary: Max return, Min drawdown, Avg volatility per ticker  

🛠 **Technologies**

- Python  
- PySpark  
- SQL  
- Delta Lake  
- Databricks Community Edition  
- Tableau Public  

🎯 **Business / Financial Insights**

- Compare cumulative return of equities vs ETFs  
- Measure risk via drawdown and rolling volatility  
- Identify high-performing stocks in a demo portfolio  
- Evaluate risk-adjusted returns over time  

👤 **Author**

Enrique Sabariego
