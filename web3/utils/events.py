import itertools

from eth_abi import (
    decode_abi,
    decode_single,
    encode_single,
)
from eth_abi.abi import (
    process_type,
)
from eth_utils import (
    encode_hex,
    event_abi_to_log_topic,
    is_list_like,
    keccak,
    to_hex,
    to_tuple,
    to_dict,
)

from web3.exceptions import (
    MismatchedABI,
)
from web3.utils.datastructures import (
    AttributeDict,
)
from web3.utils.encoding import (
    hexstr_if_str,
    to_bytes,
)
from web3.utils.normalizers import (
    BASE_RETURN_NORMALIZERS,
)
from web3.utils.toolz import (
    complement,
    compose,
    cons,
    curry,
    valfilter,
)

from .abi import (
    exclude_indexed_event_inputs,
    get_abi_input_names,
    get_indexed_event_inputs,
    map_abi_data,
    normalize_event_input_types,
)


def construct_event_topic_set(event_abi, arguments=None):
    if arguments is None:
        arguments = {}
    if isinstance(arguments, (list, tuple)):
        if len(arguments) != len(event_abi['inputs']):
            raise ValueError(
                "When passing an argument list, the number of arguments must "
                "match the event constructor."
            )
        arguments = {
            arg['name']: [arg_value]
            for arg, arg_value
            in zip(event_abi['inputs'], arguments)
        }

    normalized_args = {
        key: value if is_list_like(value) else [value]
        for key, value in arguments.items()
    }

    event_topic = encode_hex(event_abi_to_log_topic(event_abi))
    indexed_args = get_indexed_event_inputs(event_abi)
    zipped_abi_and_args = [
        (arg, normalized_args.get(arg['name'], [None]))
        for arg in indexed_args
    ]
    encoded_args = [
        [
            None if option is None else encode_hex(encode_single(arg['type'], option))
            for option in arg_options]
        for arg, arg_options in zipped_abi_and_args
    ]

    topics = [
        [event_topic] + list(permutation)
        if any(value is not None for value in permutation)
        else [event_topic]
        for permutation in itertools.product(*encoded_args)
    ]
    return topics


def construct_event_data_set(event_abi, arguments=None):
    if arguments is None:
        arguments = {}
    if isinstance(arguments, (list, tuple)):
        if len(arguments) != len(event_abi['inputs']):
            raise ValueError(
                "When passing an argument list, the number of arguments must "
                "match the event constructor."
            )
        arguments = {
            arg['name']: [arg_value]
            for arg, arg_value
            in zip(event_abi['inputs'], arguments)
        }

    normalized_args = {
        key: value if is_list_like(value) else [value]
        for key, value in arguments.items()
    }

    non_indexed_args = exclude_indexed_event_inputs(event_abi)
    zipped_abi_and_args = [
        (arg, normalized_args.get(arg['name'], [None]))
        for arg in non_indexed_args
    ]
    encoded_args = [
        [
            None if option is None else encode_hex(encode_single(arg['type'], option))
            for option in arg_options]
        for arg, arg_options in zipped_abi_and_args
    ]

    data = [
        list(permutation)
        if any(value is not None for value in permutation)
        else []
        for permutation in itertools.product(*encoded_args)
    ]
    return data


def is_dynamic_sized_type(_type):
    base_type, type_size, arrlist = process_type(_type)
    if arrlist:
        return True
    elif base_type == 'string':
        return True
    elif base_type == 'bytes' and type_size == '':
        return True
    return False


@to_tuple
def get_event_abi_types_for_decoding(event_inputs):
    """
    Event logs use the `sha3(value)` for indexed inputs of type `bytes` or
    `string`.  Because of this we need to modify the types so that we can
    decode the log entries using the correct types.
    """
    for input_abi in event_inputs:
        if input_abi['indexed'] and is_dynamic_sized_type(input_abi['type']):
            yield 'bytes32'
        else:
            yield input_abi['type']


def get_event_data(event_abi, log_entry):
    """
    Given an event ABI and a log entry for that event, return the decoded
    event data
    """
    if event_abi['anonymous']:
        log_topics = log_entry['topics']
    elif not log_entry['topics']:
        raise MismatchedABI("Expected non-anonymous event to have 1 or more topics")
    elif event_abi_to_log_topic(event_abi) != log_entry['topics'][0]:
        raise MismatchedABI("The event signature did not match the provided ABI")
    else:
        log_topics = log_entry['topics'][1:]

    log_topics_abi = get_indexed_event_inputs(event_abi)
    log_topic_normalized_inputs = normalize_event_input_types(log_topics_abi)
    log_topic_types = get_event_abi_types_for_decoding(log_topic_normalized_inputs)
    log_topic_names = get_abi_input_names({'inputs': log_topics_abi})

    if len(log_topics) != len(log_topic_types):
        raise ValueError("Expected {0} log topics.  Got {1}".format(
            len(log_topic_types),
            len(log_topics),
        ))

    log_data = hexstr_if_str(to_bytes, log_entry['data'])
    log_data_abi = exclude_indexed_event_inputs(event_abi)
    log_data_normalized_inputs = normalize_event_input_types(log_data_abi)
    log_data_types = get_event_abi_types_for_decoding(log_data_normalized_inputs)
    log_data_names = get_abi_input_names({'inputs': log_data_abi})

    # sanity check that there are not name intersections between the topic
    # names and the data argument names.
    duplicate_names = set(log_topic_names).intersection(log_data_names)
    if duplicate_names:
        raise ValueError(
            "Invalid Event ABI:  The following argument names are duplicated "
            "between event inputs: '{0}'".format(', '.join(duplicate_names))
        )

    decoded_log_data = decode_abi(log_data_types, log_data)
    normalized_log_data = map_abi_data(
        BASE_RETURN_NORMALIZERS,
        log_data_types,
        decoded_log_data
    )

    decoded_topic_data = [
        decode_single(topic_type, topic_data)
        for topic_type, topic_data
        in zip(log_topic_types, log_topics)
    ]
    normalized_topic_data = map_abi_data(
        BASE_RETURN_NORMALIZERS,
        log_topic_types,
        decoded_topic_data
    )

    event_args = dict(itertools.chain(
        zip(log_topic_names, normalized_topic_data),
        zip(log_data_names, normalized_log_data),
    ))

    event_data = {
        'args': event_args,
        'event': event_abi['name'],
        'logIndex': log_entry['logIndex'],
        'transactionIndex': log_entry['transactionIndex'],
        'transactionHash': log_entry['transactionHash'],
        'address': log_entry['address'],
        'blockHash': log_entry['blockHash'],
        'blockNumber': log_entry['blockNumber'],
    }

    return AttributeDict.recursive(event_data)


@to_tuple
def pop_singlets(seq):
    yield from (i[0] if is_list_like(i) and len(i) == 1 else i for i in seq)


@curry
def remove_trailing_from_seq(seq, remove_value=None):
    index = len(seq)
    while index > 0 and seq[index - 1] == remove_value:
        index -= 1
    return seq[:index]


normalize_topic_list = compose(
    remove_trailing_from_seq(remove_value=None),
    pop_singlets,)


def is_indexed(arg):
    if arg.is_indexed is True:
        return True


not_indexed = complement(is_indexed)


class EventFilterBuilder:
    fromBlock = None
    toBlock = None
    address = None

    def __init__(self, event_abi):
        self.event_abi = event_abi
        self.event_topic = self._initial_event_topic()
        self.args = AttributeDict(self._init_argument_filters())
        self.indexed_args = valfilter(is_indexed, self.args)
        self.data_args = valfilter(not_indexed, self.args)

    @property
    def topics(self):
        arg_topics = tuple(arg.encoded_match_values for arg in self.indexed_args.values())
        return normalize_topic_list(cons(to_hex(self.event_topic), arg_topics))

    @property
    def data_arguments(self):
        return tuple(
            arg.encoded_match_values for arg in self.data_args)

    @property
    def filter_params(self):
        params = {
            "topics": self.topics,
            "fromBlock": self.fromBlock,
            "toBlock": self.toBlock,
            "address": self.address
        }
        return valfilter(lambda x: x is not None, params)

    def deploy(self, web3):
        log_filter = web3.eth.filter(self.filter_params)
        # TODO: Data argument handling
        return log_filter

    def _initial_event_topic(self):
        if self.event_abi['anonymous'] is False:
            return event_abi_to_log_topic(self.event_abi)
        else:
            return list()

    @to_dict
    def _init_argument_filters(self):
        for item in self.event_abi['inputs']:
            key = item['name']
            value = ArgumentFilter(
                arg_type=item['type'],
                name=item['name'],
                indexed=item['indexed'])

            yield key, value


def _normalize(value):
    return value


class ArgumentFilter:
    def __init__(self, arg_type, name, indexed):
        self.arg_type = arg_type
        self.is_indexed = indexed
        self.raw_match_values = (None,)
        self.encoded_match_values = (None,)
        self.is_dynamic = is_dynamic_sized_type(self.arg_type)

    def _encode(self, value):
        encoded_value = encode_single(self.arg_type, value)
        if self.is_indexed and self.is_dynamic:
            return to_hex(keccak(encoded_value))
        else:
            return to_hex(encoded_value)

    def match_single(self, value):
        normalized = _normalize(value)
        encoded = self._encode(normalized)
        self.raw_match_values = (normalized,)
        self.encoded_match_values = (encoded,)

    def match_any(self, *values):
        normalized = tuple(_normalize(value) for value in values)
        encoded = tuple(
            self._encode(value) for value in normalized)
        self.raw_match_values = normalized
        self.encoded_match_values = encoded
