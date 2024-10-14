from typing import Iterable

from opensearchpy import OpenSearch


es_client = OpenSearch("http://localhost:9200")


def load_docs_to_elastic(
    docs: Iterable[dict], index: str, id_field: str | None = None
) -> None:
    actions_list = [{"index": {}, "doc": doc} for doc in docs]
    es_client.bulk(body=actions_list, index=index)
