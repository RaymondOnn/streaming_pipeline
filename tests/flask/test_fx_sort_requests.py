from dataclasses import dataclass


@dataclass
class FakeTransactionStream:
    string: str
    number: int
    is_fraud: bool


def test_generate_object_name():
    pass


# def test_sort_requests():
#
#     m = MagicMock(spec=minio.Minio)
#
#     with patch(
#       "src.flask_api.fx.object_store.get_client",
#       return_value=m
# ) as get_fake_minio:
#         with patch("src.flask_api.fx.pubsub.AvroProducer") as fake_producer:
#             with patch(
#               "src.flask_api.fx.registry.SchemaRegistry"
#           ) as fake_registry:
#
#                 get_fake_minio.return_value = m
#                 fake_registry_instance = fake_registry.return_value
#                 fake_producer_instance = fake_producer.return_value
#                 fake_request = json.dumps(
#                   asdict(FakeTransactionStream('a', 1, True)))
#
#                 sort_requests.sort_requests(fake_request)
#                 print(m.mock_calls, fake_producer_instance.mock_calls)
#                 assert 1 == 2
