class kafka_reader:
    def __init__(self, bootstrap_server:str, topic_name:str, starting_offsets:str, consumer_group_id:str):
        self.bootstrap_servers = bootstrap_server
        self.topic_name = topic_name
        self.starting_offsets = starting_offsets
        self.consumer_group_id = consumer_group_id

    def read_batch(self, spark):
        df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic_name)
            .option("startingOffsets", self.starting_offsets)
            .option("consumer.group.id", "kafka_reader_group")
            .load()
        )
        return df   