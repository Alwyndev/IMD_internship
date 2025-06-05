# %%
import pandas as pd
import glob

# %%
# Step 1: Load all yearly cleaned CSVs
csv_files = glob.glob("cleaned_dataset_*.csv")

numeric_cols = [
    'RVR_limited', 'MOR_limited', 'RVR_actual', 'MOR_actual',
    'BLM', 'Trf', 'Ref(v)', 'PD(v)'
]

dfs = []
for f in csv_files:
    try:
        df = pd.read_csv(f, low_memory=False)

        # Step 2: Remove bad rows (e.g., rows that contain headers as data)
        df = df[df['RVR_limited'] != 'RVR']  # Remove repeated header rows if present

        # Step 3: Convert numeric columns to float
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        dfs.append(df)

    except Exception as e:
        print(f"⚠️ Error reading {f}: {e}")

# %%
# Step 4: Concatenate all valid data
if dfs:
    df = pd.concat(dfs, ignore_index=True)
else:
    raise ValueError("No valid CSVs loaded. Please check your files.")

# %%
# Step 5: Convert 'Date' to datetime
df['Date'] = pd.to_datetime(df['Date'], format="%d.%m.%Y", errors='coerce')

# Step 6: Extract month
df['Month'] = df['Date'].dt.month

# Step 7: Filter for October to February
df_oct_to_feb = df[df['Month'].isin([11, 12, 1, 2])]

# %%
# Step 8: Save to CSV
df_oct_to_feb.to_csv("RVR_oct_to_feb_only.csv", index=False)
print(f"✅ Saved filtered data: {df_oct_to_feb.shape[0]} rows in RVR_oct_to_feb_only.csv")
