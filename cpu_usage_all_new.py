import math

import pandas as pd
import matplotlib.pyplot as plt
import os
import json


def extract_times_from_simulation_log(file_path, protocol):
    print(f"Extracting times from {file_path}")
    with open(file_path, 'r') as file:
        lines = file.readlines()

    first_request_time = None
    last_request_time = None

    def extract_timestamp(line):
        parts = line.split()
        try:
            if protocol == 'thrift':
                if len(parts) > 3 and parts[0] == "REQUEST":
                    start_time = int(parts[2])
                    end_time = int(parts[3])
                    return start_time, end_time
            elif protocol == "RabbitMQ async":
                if len(parts) > 3 and parts[0] == "REQUEST":
                    start_time = int(parts[5])
                    end_time = int(parts[6])
                    return start_time, end_time
            else:
                if len(parts) > 3 and parts[0] == "REQUEST":
                    start_time = int(parts[2])
                    end_time = int(parts[3])
                    return start_time, end_time
        except ValueError:
            return None

    for line in lines:
        if "REQUEST" in line:
            timestamp = extract_timestamp(line)
            if timestamp:
                first_request_time = min(first_request_time, timestamp[0]) if first_request_time else timestamp[0]
                last_request_time = max(last_request_time, timestamp[1]) if last_request_time else timestamp[1]

    if first_request_time and last_request_time:
        first_request_time = pd.to_datetime(first_request_time, unit='ms')
        last_request_time = pd.to_datetime(last_request_time, unit='ms')
        first_request_time = first_request_time.tz_localize('UTC').tz_convert('Europe/Warsaw').tz_localize(None)
        last_request_time = last_request_time.tz_localize('UTC').tz_convert('Europe/Warsaw').tz_localize(None)

    first_request_time += pd.Timedelta(seconds=10)
    return first_request_time, last_request_time



def calculate_average_cpu_usage(file_path, start_time, end_time, max_cpu=1):
    cpu_data = pd.read_csv(file_path)
    if 'Process CPU Usage' not in cpu_data.columns or 'Time' not in cpu_data.columns:
        return None, None

    cpu_data['Time'] = pd.to_datetime(cpu_data['Time'])
    if cpu_data['Time'].min() > pd.to_datetime(end_time):
        return None, None


    filtered_data = cpu_data[(cpu_data['Time'] >= start_time) & (cpu_data['Time'] <= end_time)]
    filtered_data = filtered_data[filtered_data['Process CPU Usage'] > 0].dropna(subset=['Process CPU Usage'])

    total_seconds = (pd.to_datetime(end_time) - filtered_data['Time'].min()).total_seconds() + 1


    missing_entries = total_seconds - len(filtered_data)

    total_cpu_used = filtered_data['Process CPU Usage'].sum() + missing_entries * max_cpu

    average_cpu_usage = (total_cpu_used / (len(filtered_data) + missing_entries)) * 100

    return round(average_cpu_usage, 2), filtered_data['Time'].min()


def process_protocol(base_directory, experiment):
    protocols = ['rest', 'grpc', 'thrift', 'RabbitMQ sync', 'RabbitMQ async', 'kafka sync', 'kafka async']
    # protocols = ['grpc']
    microservices = ['M1', 'M2']
    results = {}
    protocol_averages = {microservice: [] for microservice in microservices}
    protocol_labels = ['REST', 'gRPC', 'Thrift', 'RabbitMQ sync', 'RabbitMQ async', 'Kafka sync', 'Kafka async']

    for protocol in protocols:
        protocol_data = {}
        for microservice in microservices:
            path = os.path.join(base_directory, protocol, experiment)
            microservice_data = {"runs": []}

            for i in range(1, 4):
                run_path = os.path.join(path, str(i))
                log_dir = next((d for d in os.listdir(run_path) if 'constantuserstests-' in d), None)
                log_file_path = os.path.join(run_path, log_dir, 'simulation.log')

                if os.path.exists(log_file_path):
                    start_time, end_time = extract_times_from_simulation_log(log_file_path, protocol)
                    print(f"Duration: {end_time - start_time}")
                    microservice_path = os.path.join(run_path, microservice)
                    cpu_files = [f for f in os.listdir(microservice_path) if
                                 f.startswith('CPU Usage') and f.endswith('.csv')]

                    run_data = {"instances": []}
                    total_avg_usage = 0
                    if protocol == 'grpc' and i == 1 and microservice == 'M1' and experiment == '100u1000p':
                        continue
                    for csv_file in cpu_files:
                        full_path = os.path.join(microservice_path, csv_file)
                        avg_usage, instance_start_time = calculate_average_cpu_usage(full_path, start_time, end_time)
                        if avg_usage is not None and not math.isnan(avg_usage):
                            instance_data = {
                                "started_at": instance_start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                "average_cpu_usage": avg_usage,
                                "file_path": full_path,
                            }

                            run_data["instances"].append(instance_data)
                            total_avg_usage += avg_usage

                    run_data["total_average_cpu_usage"] = round(total_avg_usage, 2)
                    microservice_data["runs"].append(run_data)

            protocol_data[microservice] = microservice_data
            avg = sum([run["total_average_cpu_usage"] for run in microservice_data["runs"]]) / len(
                microservice_data["runs"])
            microservice_data["runs"].append({microservice: round(avg, 2)})
            protocol_averages[microservice].append(avg)

        results[protocol] = protocol_data


    # Print structured data
    print(json.dumps(results, indent=4))

    # Plot the data
    x = range(len(protocol_labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 8))
    bars1 = ax.bar(x, protocol_averages['M1'], width, label='Mikroserwis 1')
    bars2 = ax.bar([p + width for p in x], protocol_averages['M2'], width, label='Mikroserwis 2')


    ax.set_xlabel('Mechanizm komunikacji')
    ax.set_ylabel('Średnie użycie CPU (%)')
    ax.set_title('Średnie użycie CPU przez mechanizm komunikacji')
    ax.set_xticks([p + width / 2 for p in x])
    ax.set_xticklabels(protocol_labels)
    ax.legend()


    for bar in bars1 + bars2:
        height = bar.get_height()
        ax.annotate(f'{height:.2f}%',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom')

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# Main configuration
base_directory = 'D:\\OneDrive - Politechnika Wroclawska\\magisterka\\wyniki\\ConstantUsers'
experiment = '500u1000p'

# Process each protocol
process_protocol(base_directory, experiment)
