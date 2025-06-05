import os
import pandas as pd

def read_custom_file(file_path):
    """Read and parse a single RVR data file."""
    try:
        with open(file_path, 'r') as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
        
        # Find where data starts (skip all header lines until we find "Time" column)
        data_start = 0
        for i, line in enumerate(lines):
            if line.startswith("Time\t"):
                data_start = i
                break
        
        # Process data lines
        data_lines = []
        for line in lines[data_start + 1:]:  # Skip header line
            parts = [x for x in line.split('\t') if x]
            if len(parts) >= 9:
                data_lines.append(parts[:9])
        
        if not data_lines:
            print(f"Skipped (no valid data): {file_path}")
            return None
            
        df = pd.DataFrame(data_lines, columns=[
            'Time', 'RVR_limited', 'MOR_limited', 'RVR_actual', 'MOR_actual',
            'BLM', 'Trf', 'Ref(v)', 'PD(v)'
        ])
        return df
        
    except Exception as e:
        print(f"Error reading {file_path}: {str(e)}")
        return None

# Main processing
years = ['2016', '2017', '2018', '2019']

for year in years:
    root_dir = f'RVR/{year}'
    output_data = []
    
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".txt"):
                filepath = os.path.join(root, file)
                df = read_custom_file(filepath)
                
                if df is not None:
                    # Extract metadata from path (adjust indices based on your folder structure)
                    parts = os.path.normpath(filepath).split(os.sep)
                    try:
                        runway = parts[-3]  # Adjust if needed
                        date = os.path.splitext(file)[0]  # Removes .txt
                    except IndexError:
                        runway = "Unknown"
                        date = "Unknown"
                    
                    df['Runway'] = runway
                    df['Date'] = date
                    df['Year'] = year
                    output_data.append(df)

    # Save yearly data
    if output_data:
        final_df = pd.concat(output_data, ignore_index=True)
        final_df.to_csv(f"cleaned_dataset_{year}.csv", index=False)
        print(f"✅ {year}: Saved {len(final_df)} rows to cleaned_dataset_{year}.csv")
    else:
        print(f"⚠️ {year}: No valid data found")

print("Processing complete.")