from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import json

# Connection settings
es = Elasticsearch(
    ['https://localhost:9200'],
    http_auth=('elastic', 'Hc1ME0C48V827KEKf71ziI6Q'),
    verify_certs=False
)

# Date and time inputs
start_datetime_input = "2024-06-10T22:19:52"
start_datetime = datetime.strptime(start_datetime_input, "%Y-%m-%dT%H:%M:%S")
end_datetime = start_datetime + timedelta(minutes=5.1)

# Elasticsearch query
query = {
    "query": {
        "range": {
            "startTime": {
                "gte": int(start_datetime.timestamp() * 1e6),  # Convert to microseconds
                "lte": int(end_datetime.timestamp() * 1e6)
            }
        }
    },
    "sort": [{"startTime": {"order": "asc"}}, "_doc"],  # Add _doc to maintain a consistent order
    "size": 10000
}

index_name = "my-prefix-jaeger-span-2024-06-10"
search_after = None
traces = {}


def format_span(span):
    return {
        "traceID": span['_source'].get('traceID'),
        "spanID": span['_source'].get('spanID'),
        "operationName": span['_source'].get('operationName'),
        "startTime": span['_source'].get('startTime'),
        "duration": span['_source'].get('duration'),
        "tags": [{"key": tag['key'], "value": tag.get('value')} for tag in span['_source'].get('tags', [])],
        "logs": span['_source'].get('logs', []),
        "process": span['_source']['process'],
        "references": span['_source'].get('references')
    }


def sort_and_resolve_references(trace):
    # Sort spans by startTime primarily
    trace['spans'].sort(key=lambda x: x['startTime'])

    # Map of spanID to span for easy access
    span_id_to_span = {span['spanID']: span for span in trace['spans']}
    sorted_spans = []
    visited = set()

    def visit(span_id):
        if span_id in visited:
            return
        visited.add(span_id)
        span = span_id_to_span[span_id]
        # Traverse child spans first, if they exist
        if 'references' in span:
            for ref in span['references']:
                if ref['refType'] == 'CHILD_OF' and ref['spanID'] in span_id_to_span:
                    visit(ref['spanID'])
        sorted_spans.append(span)

    # Find root spans - those not referenced as a child of any other span
    root_spans = [s for s in trace['spans'] if all(ref['spanID'] != s['spanID'] for ref in s.get('references', []))]

    # Visit each root span to construct the sorted list
    for root_span in root_spans:
        visit(root_span['spanID'])

    # Assign sorted spans back to the trace
    trace['spans'] = sorted_spans


while True:
    if search_after:
        query['search_after'] = search_after
    response = es.search(index=index_name, body=query)
    if not response['hits']['hits']:
        break
    for hit in response['hits']['hits']:
        formatted_span = format_span(hit)
        trace_id = formatted_span['traceID']
        if trace_id not in traces:
            traces[trace_id] = {"spans": [], "processes": {}}
        traces[trace_id]["spans"].append(formatted_span)
        service_name = formatted_span['process']['serviceName']
        if service_name not in traces[trace_id]["processes"]:
            traces[trace_id]["processes"][service_name] = formatted_span['process']
    print(f"Traces found: {len(traces)}")
    search_after = response['hits']['hits'][-1]['sort']


print(f"Final Traces found: {len(traces)}. Sorting and resolving references...")
for trace in traces.values():
    sort_and_resolve_references(trace)


# Formatting the final output
formatted_output = [{"traceID": trace_id, "spans": trace["spans"], "processes": trace["processes"]} for trace_id, trace
                    in traces.items()]

# Saving the output to a file
output_file_path = 'output_data.json'
with open(output_file_path, 'w') as f:
    json.dump({"data": formatted_output}, f)

print(f"Data successfully saved to {output_file_path}")
