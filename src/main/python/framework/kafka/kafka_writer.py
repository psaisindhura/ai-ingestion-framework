class kafka_writer:
    def __init__(self, boostrap_server:str, topic_name:str):
        self.bootstrap_servers = boostrap_server
        self.topic_name = topic_name
    def write_to_kafka(self, df):
        (
            df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("topic", self.topic_name)
            .save()
            
        )
        