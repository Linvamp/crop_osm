from kedro.pipeline import node, Pipeline
from .nodes import soi_osm_crop

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=soi_osm_crop,
                inputs=["Limits_csv", "s3_credentials"],
                outputs="clipped_file_csv",
                name="soi_osm_crop",
            )
        ]
    )