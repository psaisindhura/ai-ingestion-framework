class kafka_reader:
    def __init__(self, bootstrap_server:str, topic_name:str, starting_offsets:str):
        self.bootstrap_servers = bootstrap_server
        self.topic_name = topic_name
        self.starting_offsets = starting_offsets

    def read_batch(self, spark):
        df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic_name)
            .option("startingOffsets", self.starting_offsets)
            .load()
        )
        return df   