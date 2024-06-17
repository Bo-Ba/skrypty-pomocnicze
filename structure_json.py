import json


def load_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


def save_json(data, file_path):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)


def main(input_file, output_file):
    data = load_json(input_file)

    save_json(data, output_file)
    print(f"Indented JSON data has been saved to {output_file}")


if __name__ == "__main__":
    input_file = "D:\\OneDrive - Politechnika Wroclawska\\magisterka\\wyniki\\ConstantUsers\\RabbitMQ async\\500u10p\\2\\output_data.json"
    output_file = "structured.json"
    main(input_file, output_file)
