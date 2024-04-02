from kedro.pipeline import node, Pipeline
from .nodes import soi_osm_crop

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=soi_osm_crop,
                inputs=["Limits_csv", "ugixsoi_s3_cred", "ugixsoiprocessed_s3_cred"],
                outputs="clipped_file_csv",
                name="soi_osm_crop",
            )
        ]
    )