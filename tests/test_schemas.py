from src.schemas import Event, EventIn


def test_event_roundtrips_through_kafka_json():
    event = Event(event_type="order.created", payload={"amount": 42}, source="test")

    raw = event.to_kafka_json()
    restored = Event.from_kafka_json(raw)

    assert restored.id == event.id
    assert restored.event_type == event.event_type
    assert restored.payload == event.payload
    assert restored.produced_at == event.produced_at


def test_event_in_defaults_source_to_api():
    event_in = EventIn(event_type="user.signup", payload={})
    assert event_in.source == "api"
