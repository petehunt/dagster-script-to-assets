import os
import pandas
from hackernews import extract, transform, load
from dagster import asset, RetryPolicy, FreshnessPolicy, IOManager, Definitions, fs_io_manager, job, op, DagsterInstance, file_relative_path, AssetKey, OpExecutionContext, AssetMaterialization, Output
from dagster._core.storage.fs_io_manager import PickledObjectFilesystemIOManager
from dagster._experimental.branching_io_manager import BranchingIOManager

# class HackerNewsIOManager(IOManager):
#     def load_input(self, context, input):
#         raise NotImplementedError()

#     def handle_output(self, context, md_content):
#         load(md_content)


@asset
def hackernews_source_data():
    return extract()


@asset(
    retry_policy=RetryPolicy(max_retries=5, delay=5),
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=30),
    # io_manager_key="hn_io_manager",
)
def hackernews_wordcloud(hackernews_source_data: pandas.DataFrame):
    return transform(hackernews_source_data)


prod_io_manager = PickledObjectFilesystemIOManager(base_dir="prod")
dev_io_manager = PickledObjectFilesystemIOManager(base_dir="dev")

def is_prod():
    return os.getenv("DAGSTER_DEPLOYMENT") == "PROD"

io_manager = prod_io_manager if is_prod() else BranchingIOManager(parent_io_manager=prod_io_manager, branch_io_manager=dev_io_manager)


@op
def replicate_asset_materializations_for_group(context: OpExecutionContext) -> None:
    assets_to_ship = ["hackernews_source_data",  "hackernews_wordcloud"]

    # TODO switch to right instance folder
    prod_instance = DagsterInstance.from_config(file_relative_path(__file__, "prod_dagster_home"))

    for asset_key_str in assets_to_ship:
        asset_key = AssetKey(asset_key_str)
        latest_event_log_entry = prod_instance.get_latest_materialization_event(asset_key)
        if latest_event_log_entry:
            asset_mat = latest_event_log_entry.asset_materialization
            if asset_mat:
                context.log.info(f"Inserting {latest_event_log_entry} into dev with metadata {asset_mat.metadata}")
                yield AssetMaterialization(
                    asset_key=asset_key_str, 
                    description=asset_mat.description, 
                    metadata={**asset_mat.metadata, **{
                        "parent_asset_catalog_url": f"path/to/url/assets/{asset_key_str}",
                        "parent_run_id": latest_event_log_entry.run_id,
                        "parent_timestamp": latest_event_log_entry.timestamp,
                    }},
                    # metadata_entries=asset_mat.metadata_entries, 
                    partition=asset_mat.partition, 
                    tags=asset_mat.tags
                )
            # dev_instance._event_storage.store_event(latest_event_log_entry)
        else:
            context.log.info(f"Did not find entry in catalog for {asset_key_str} in prod instance.")

    yield Output(value=None)
    

@job
def ship_asset_materializations():
    replicate_asset_materializations_for_group()

dev_defs = Definitions(
    assets=[hackernews_source_data, hackernews_wordcloud],
    jobs=[] if is_prod() else [ship_asset_materializations],
    resources={
        "io_manager" : io_manager,
        # "hn_io_manager": HackerNewsIOManager(),
    }
)
