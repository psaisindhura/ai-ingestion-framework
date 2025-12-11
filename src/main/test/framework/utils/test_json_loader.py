from framework.utils.json_loader import JsonLoader

loader = JsonLoader("/opt/ai-ingestion-framework/ai-ingestion-framework/configs/job_config.json")

print(loader.source_path)        # /data/input/
print(loader.input_file_type)    # csv
print(loader.transform)      # status
print(loader.destination_path)   # /data/output/
print(loader.output_file_type)   # parquet

loader.print_config() 