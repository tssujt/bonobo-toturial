import bonobo
import requests
from bonobo.config import use, use_context_processor


def with_opened_file(self, context):
    with context.get_service('fs').open('output.txt', 'w+') as f:
        yield f


@use('http')
def extract(http):
    yield from http.get('https://public-us.opendatasoft.com/api/records/1.0/search/?dataset=fablabs&rows=10') \
        .json() \
        .get('records')


@use_context_processor(with_opened_file)
def write_repr_to_file(f, *row):
    f.write(repr(row) + "\n")


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(extract, bonobo.PrettyPrinter(), write_repr_to_file)

    return graph


def get_services(**options):
    http = requests.Session()
    http.headers = {'User-Agent': 'Monkeys!'}
    return {
        'http': http,
        'fs': bonobo.open_fs()
    }


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
