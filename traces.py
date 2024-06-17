import json
import numpy as np
import os
import glob
from scipy.stats import describe
# microseconds
max_duration = 60000000

def parse_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


def choose_unit(durations):
    if not durations:
        return 1, 'μs'
    average_duration = np.mean(durations)
    if average_duration < 1000:
        return 1, 'μs'
    elif average_duration < 1000000:
        return 1000, 'ms'
    else:
        return 1000000, 's'
    return 1000, 'ms'


def filter_spans(data, protocol):
    durations = {'SUCCESS': [], 'FAILURE': []}
    if protocol == 'RabbitMQ async':
        handle_rabbitmq_async(data, durations)
    elif protocol == 'Kafka async':
        handle_kafka_async(data, durations)
    else:
        handle_standard_protocols(data, durations, protocol)
    return durations


def handle_kafka_async(data, durations):
    async_spans = {}
    for trace in data['data']:
        if len(trace['spans']) < 2:
            continue

        trace_id = trace['traceID']
        start_span = trace['spans'][1]
        end_span = None

        try:
            end_span = trace['spans'][4]
        except IndexError:
            if trace_id not in async_spans:
                async_spans[trace_id] = {}
            async_spans[trace_id]['end'] = None
            continue

        if start_span['operationName'] == "events.requests send":
            if trace_id not in async_spans:
                async_spans[trace_id] = {}
            async_spans[trace_id]['start'] = start_span['startTime'] / 1000.0
        else:
            if trace_id not in async_spans:
                async_spans[trace_id] = {}
            async_spans[trace_id]['start'] = None

        if end_span['operationName'] == "events.responses receive":
            if trace_id not in async_spans:
                async_spans[trace_id] = {}
            async_spans[trace_id]['end'] = (end_span['startTime'] + end_span['duration']) / 1000.0
        else:
            if trace_id not in async_spans:
                async_spans[trace_id] = {}
            async_spans[trace_id]['end'] = None

    calculate_async_durations(async_spans, durations)


def handle_rabbitmq_async(data, durations):
    async_spans = {}
    for trace in data['data']:
        if len(trace['spans']) < 2:
            continue

        trace_id = trace['traceID']
        start_span = trace['spans'][1]
        end_span = None

        try:
            end_span = trace['spans'][4]
        except IndexError:
            if trace_id not in async_spans:
                async_spans[trace_id] = {}
            async_spans[trace_id]['end'] = None
            continue

        if start_span['operationName'] == "events/requests send":
            if trace_id not in async_spans:
                async_spans[trace_id] = {}
            async_spans[trace_id]['start'] = start_span['startTime'] / 1000.0
        else:
            if trace_id not in async_spans:
                async_spans[trace_id] = {}
            async_spans[trace_id]['start'] = None

        if end_span['operationName'] == "responses receive":
            if trace_id not in async_spans:
                async_spans[trace_id] = {}
            async_spans[trace_id]['end'] = (end_span['startTime'] + end_span['duration']) / 1000.0
        else:
            if trace_id not in async_spans:
                async_spans[trace_id] = {}
            async_spans[trace_id]['end'] = None

    calculate_async_durations(async_spans, durations)


def calculate_async_durations(async_spans, durations):
    for times in async_spans.values():
        if 'start' in times and 'end' in times:
            if times['end'] is not None or times['start'] is not None:
                try:
                    total_duration = times['end'] - times['start']
                except TypeError:
                    durations['FAILURE'].append(max_duration + 1)
                if 0 < total_duration < max_duration:
                    durations['SUCCESS'].append(total_duration)
                else:
                    durations['FAILURE'].append(max_duration + 1)
            else:
                durations['FAILURE'].append(max_duration + 1)
        else:
            durations['FAILURE'].append(max_duration + 1)


def handle_standard_protocols(data, durations, protocol):
    for trace in data['data']:
        classify_span(trace, durations, protocol)


def classify_span(trace, durations, protocol):
    if len(trace['spans']) < 2:
        return
    span = trace['spans'][1]
    operation, service = span['operationName'], span['process']['serviceName']
    if protocol == 'rest' and operation == "http get" and service == "microservice1":
        classify_by_outcome(span, durations)
    elif protocol == 'grpc' and operation == "ExperimentService/getResponse" and service == "microservice1":
        classify_by_grpc_status(span, durations)
    elif protocol == 'thrift' and operation == "thrift client getPayload" and service == "microservice1":
        is_failure = any(
            field['key'] == 'exception.type' and field['value'] == 'org.apache.thrift.transport.TTransportException'
            for log in trace['spans'][0]['logs']
            for field in log['fields']
        )
        if span['duration'] > max_duration or is_failure:
            durations['FAILURE'].append(span['duration'])
        else:
            durations['SUCCESS'].append(span['duration'])
    elif protocol == 'RabbitMQ sync' and operation == "rabbit rpc request" and service == "microservice1":
        classify_by_error_tag(span, durations)
    elif protocol == 'Kafka sync' and operation == "kafka-producer#get-payload" and service == "microservice1":
        classify_by_error_tag(span, durations)


def classify_by_outcome(span, durations):
    outcome = next((tag['value'] for tag in span['tags'] if tag['key'] == 'outcome'), 'FAILURE')
    durations['SUCCESS' if outcome == 'SUCCESS' else 'FAILURE'].append(span['duration'])


def classify_by_grpc_status(span, durations):
    grpc_status = next((tag['value'] for tag in span['tags'] if tag['key'] == 'grpc.status_code'), 'UNKNOWN')
    durations['SUCCESS' if grpc_status == 'OK' else 'FAILURE'].append(span['duration'])


def classify_by_error_tag(span, durations, is_success='true'):
    error_tag = next((tag['value'] for tag in span['tags'] if tag['key'] == 'error'), 'false')
    if error_tag == 'false' and span['duration'] <= max_duration:
        durations['SUCCESS'].append(span['duration'])
    else:
        durations['FAILURE'].append(span['duration'])


def compute_statistics(durations):
    if not durations:
        return {'count': 0, 'min': '-', 'max': '-', 'mean': '-', 'std_dev': '-', '50th': '-', '75th': '-', '95th': '-',
                '99th': '-'}
    stats = describe(durations)
    percentiles = np.percentile(durations, [50, 75, 95, 99])
    return {
        'count': int(stats.nobs),
        'min': round(np.min(durations), 2),
        'max': round(np.max(durations), 2),
        'mean': round(np.mean(durations), 2),
        'std_dev': round(np.std(durations, ddof=1), 2),
        '50th': round(percentiles[0], 2),
        '75th': round(percentiles[1], 2),
        '95th': round(percentiles[2], 2),
        '99th': round(percentiles[3], 2)
    }


def generate_report(durations, protocol_name):
    unit_factor, unit_name = choose_unit(durations['SUCCESS'] + durations['FAILURE'])
    stats_success = compute_statistics([d / unit_factor for d in durations['SUCCESS']])
    stats_failure = compute_statistics([d / unit_factor for d in durations['FAILURE']])
    total_requests = stats_success['count'] + stats_failure['count']

    print(f"\n---- Global Information for {protocol_name} --------------------------------------------------------")
    print(f"> Unit of measurement: {unit_name}")
    print(f"> Request count: {total_requests} (OK={stats_success['count']} KO={stats_failure['count']})")
    print(f"> Min response time: {stats_success['min']} (OK={stats_success['min']} KO={stats_failure['min']})")
    print(f"> Max response time: {stats_success['max']} (OK={stats_success['max']} KO={stats_failure['max']})")
    print(f"> Mean response time: {stats_success['mean']} (OK={stats_success['mean']} KO={stats_failure['mean']})")
    print(f"> Std deviation: {stats_success['std_dev']} (OK={stats_success['std_dev']} KO={stats_failure['std_dev']})")
    print(
        f"> Response time 50th percentile: {stats_success['50th']} (OK={stats_success['50th']} KO={stats_failure['50th']})")
    print(
        f"> Response time 75th percentile: {stats_success['75th']} (OK={stats_success['75th']} KO={stats_failure['75th']})")
    print(
        f"> Response time 95th percentile: {stats_success['95th']} (OK={stats_success['95th']} KO={stats_failure['95th']})")
    print(
        f"> Response time 99th percentile: {stats_success['99th']} (OK={stats_success['99th']} KO={stats_failure['99th']})")
    print(
        f"> Mean requests/sec: {stats_success['count']/900:.4f}")


def process_protocol(base_directory, experiment):
    # protocols = ['rest', 'grpc', 'thrift', 'RabbitMQ sync', 'RabbitMQ async', 'Kafka sync', 'Kafka async']
    protocols = ['RabbitMQ async']
    for protocol in protocols:
        print(f"Processing protocol: {protocol}")
        path = os.path.join(base_directory, protocol, experiment)
        aggregate_durations = {'SUCCESS': [], 'FAILURE': []}

        for i in range(1, 4):
            run_path = os.path.join(path, str(i))
            json_files = glob.glob(os.path.join(run_path, "*.json"))
            # if protocol == 'RabbitMQ async' and i == 1:
            #     continue
            for file in json_files:
                print(f"Processing file: {file}")
                data = parse_data(file)
                durations = filter_spans(data, protocol)
                generate_report(durations, f"{protocol} Run {i}")
                aggregate_durations['SUCCESS'].extend(durations['SUCCESS'])
                aggregate_durations['FAILURE'].extend(durations['FAILURE'])
                with open(f"{i}.json", 'w') as f:
                    json.dump(durations, f)

        # Generate aggregated report for all runs of each protocol
        generate_report(aggregate_durations, f"Total {protocol}")


def main():
    base_directory = 'D:\\OneDrive - Politechnika Wroclawska\\magisterka\\wyniki\\ConstantUsers'
    experiment = '500u10p'
    process_protocol(base_directory, experiment)


if __name__ == "__main__":
    main()
