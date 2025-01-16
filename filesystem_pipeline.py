import dlt
from dlt.sources.filesystem import filesystem, read_parquet

filesystem_resource = filesystem(
  bucket_url="data",
  file_glob="**/*.parquet"
)
filesystem_pipe = filesystem_resource | read_parquet()

# We load the data into the table_name table
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
load_info = pipeline.run(filesystem_pipe.with_name("userdata"))
print(load_info)