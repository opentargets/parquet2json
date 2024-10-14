from typing import Iterable

from opensearchpy import OpenSearch


def load_docs_to_elastic(
    docs: Iterable[dict], uri: str, index: str, id_field: str | None = None
) -> None:
    es_client = OpenSearch(uri)
    actions_list = [{"index": {}, "doc": doc} for doc in docs]
    es_client.bulk(body=actions_list, index=index)
