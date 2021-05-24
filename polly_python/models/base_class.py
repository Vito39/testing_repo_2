from json import dumps

from pandas import DataFrame


class BaseClass:
    def to_json(self) -> str:
        return dumps(vars(self))

    def to_df(self) -> DataFrame:
        return DataFrame(vars(self), index=[0])

    @classmethod
    def from_api_response(cls, response):
        raise NotImplementedError()
