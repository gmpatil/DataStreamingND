import producer_server


def run_kafka_server():
    # Get the json file path
    input_file = "/home/workspace/police-department-calls-for-service.json"

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="org.sfopd.cs.events.1",
        bootstrap_servers="localhost:9092",
        client_id="provider-001"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
