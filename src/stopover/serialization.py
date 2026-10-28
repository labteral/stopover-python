"""Read legacy Pickle messages without importing executable globals."""
from collections import OrderedDict
import io
import pickle
import pickletools


class MessageUnpickler(pickle.Unpickler):
    def __init__(self, value, classes):
        for opcode, _, position in pickletools.genops(value):
            if opcode.name in ('EXT1', 'EXT2', 'EXT4'):
                raise pickle.UnpicklingError(
                    'Pickle extensions are not allowed')
        if position + 1 != len(value):
            raise pickle.UnpicklingError('Trailing message pickle data')
        super().__init__(io.BytesIO(value))
        self.allowed = {(cls.__module__, cls.__name__): cls
                        for cls in (OrderedDict, *classes)}

    def find_class(self, module, name):
        try:
            return self.allowed[module, name]
        except KeyError:
            raise pickle.UnpicklingError(
                f'Unapproved message class: {module}.{name}') from None
