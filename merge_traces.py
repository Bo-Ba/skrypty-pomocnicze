import json
import os

def merge_jaeger_traces(directory, output_file):
    merged_data = {"data": []}

    for filename in os.listdir(directory):
        if filename.startswith("traces-") and filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r') as f:
                data = json.load(f)
                if "data" in data:
                    merged_data["data"].extend(data["data"])
                else:
                    print(f"Warning: No 'data' key found in {file_path}")

    with open(output_file, 'w') as f:
        json.dump(merged_data, f)

    print(f"Successfully merged files from {directory} into {output_file}")

# Directory containing Jaeger tracing files
directory = r"D:\OneDrive - Politechnika Wroclawska\magisterka\wyniki\ConstantUsers\rest\100u10p\3"
output_file = r"D:\OneDrive - Politechnika Wroclawska\magisterka\wyniki\ConstantUsers\rest\100u10p\3\merged_traces.json"

# Merge the files
merge_jaeger_traces(directory, output_file)
