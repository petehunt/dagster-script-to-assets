from hackernews import extract, transform, load
from dagster import asset, RetryPolicy, FreshnessPolicy, IOManager, Definitions


class HackerNewsIOManager(IOManager):
    def load_input(self, context, input):
        raise NotImplementedError()

    def handle_output(self, context, md_content):
        load(md_content)


@asset
def hackernews_source_data():
    return extract()


@asset(
    retry_policy=RetryPolicy(max_retries=5, delay=5),
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=30),
    io_manager_key="hn_io_manager",
)
def hackernews_wordcloud(hackernews_source_data):
    return transform(hackernews_source_data)


defs = Definitions(
    assets=[hackernews_source_data, hackernews_wordcloud],
    resources={"hn_io_manager": HackerNewsIOManager()},
)
