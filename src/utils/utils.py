__all__ = ["get_random_color", "get_feeder_network"]

from zepben.evolve import NetworkConsumerClient, NetworkService


async def get_feeder_network(channel, feeder_mrid):
    client = NetworkConsumerClient(channel)
    network = NetworkService()
    (await client.get_feeder(network, mrid=feeder_mrid)).throw_on_error()
    return network


def get_random_color():
    count = 0
    colors = [
        "#696969",
        "#2e8b57",
        "#7f0000",
        "#808000",
        "#000080",
        "#ff0000",
        "#ff8c00",
        "#ffd700",
        "#ba55d3",
        "#00ff7f",
        "#0000ff",
        "#f08080",
        "#adff2f",
        "#ff00ff",
        "#1e90ff",
        "#dda0dd",
        "#87ceeb",
        "#ff1493",
        "#7fffd4",
        "#ffe4c4"
    ]

    while True:
        count = (count + 1) % len(colors)
        yield colors[count]