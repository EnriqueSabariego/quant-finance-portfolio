import pandas as pd
import os

# 1 Ruta del archivo
downloads_folder = os.path.expanduser("~/Downloads/archive")
file_path = os.path.join(downloads_folder, "all_stock_data.csv")

# 2 Cargar CSV completo
df = pd.read_csv(file_path)

# 3 Elegir solo los tickers que quieres para tu portfolio
tickers = ["AAPL", "MSFT", "SPY"]
df_filtered = df[df["Ticker"].isin(tickers)]

# 4 Filtrar fechas desde 2018-01-01
df_filtered["Date"] = pd.to_datetime(df_filtered["Date"])
df_filtered = df_filtered[df_filtered["Date"] >= "2018-01-01"]

# 5 Ordenar por ticker y fecha
df_filtered = df_filtered.sort_values(by=["Ticker", "Date"])

# 6 Guardar CSV filtrado
output_path = os.path.join(downloads_folder, "stock_prices_portfolio.csv")
df_filtered.to_csv(output_path, index=False)

print(f"Archivo filtrado creado: {output_path}")
print(f"Filas finales: {len(df_filtered)}")
