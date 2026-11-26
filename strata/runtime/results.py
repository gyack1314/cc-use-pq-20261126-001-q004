"""允许以属性方式读取键的字典，兼容两种现场工具调用风格。"""


class Result(dict):
    """键均为合法标识符时可通过属性访问；与普通 dict 完全相等。"""

    def __getattr__(self, name: str):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    def as_dict(self) -> dict:
        def convert(value):
            if isinstance(value, Result):
                return {key: convert(item) for key, item in value.items()}
            if isinstance(value, list):
                return [convert(item) for item in value]
            if isinstance(value, tuple):
                return [convert(item) for item in value]
            return value

        return convert(self)


def result(**fields) -> Result:
    return Result(fields)


class PartRef(str):
    """既是分片摘要字符串，又可按 {sha256,digest,size} 记录读取。

    用于兼容两种凭据 parts 形态；JSON 中仍序列化为摘要字符串。
    """

    def __new__(cls, digest: str, size: int | None = None):
        obj = str.__new__(cls, digest)
        obj.size = size
        return obj

    def __getitem__(self, key):
        if isinstance(key, str):
            if key in ("sha256", "digest"):
                return str(self)
            if key == "size":
                return self.size
            raise KeyError(key)
        return str.__getitem__(self, key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    @property
    def digest(self) -> str:
        return str(self)

    @property
    def sha256(self) -> str:
        return str(self)

    def as_dict(self) -> dict:
        return {"sha256": str(self), "digest": str(self), "size": self.size}
