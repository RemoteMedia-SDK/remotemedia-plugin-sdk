import numpy as np


class PassThrough:
    def process_shared(self, data, ctx):
        return data

    def process(self, data):
        return data


class Gain:
    def process_shared(self, data, ctx):
        loan = ctx.allocate_audio(len(data['samples']))
        output = np.asarray(loan)
        pointer = output.__array_interface__['data'][0]
        np.multiply(data['samples'], np.float32(0.5), out=output)
        del output
        return {
            **data,
            'samples': np.asarray(loan.publish()),
            'metadata': {'producer_address': pointer},
        }


class Chunks:
    def process_streaming_shared(self, data, ctx):
        for start in (0, 4, 8):
            yield {**data, 'samples': data['samples'][start:start + 4]}


class Unpublished:
    def process_shared(self, data, ctx):
        loan = ctx.allocate_audio(4)
        output = np.asarray(loan)
        output.setflags(write=False)
        return {**data, 'samples': output}


class PrivateOutput:
    def process_shared(self, data, ctx):
        output = data['samples'] * np.float32(2)
        output.setflags(write=False)
        return {**data, 'samples': output}


class BadMetadata:
    def process_shared(self, data, ctx):
        return {**data, 'metadata': {'hidden_payload': np.ones(4, dtype=np.float32)}}


class BadLayout:
    def process_shared(self, data, ctx):
        return {**data, 'channels': 3}

    def process_streaming_shared(self, data, ctx):
        yield {**data, 'channels': 3}
